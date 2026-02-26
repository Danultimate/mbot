"""
Matchbook Automated Trading Bot.
Background async process running Phase 1 (Scalping) and Phase 2 (Market Making) logic.
Implements Green Up formula, Lay liability check, and market suspension retry.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import config
import db
from matchbook_api import MatchbookAPI, MarketSuspendedError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# FORMULA 1: Green Up / Exit Stake
# Lay Stake = (Back Stake * Back Odds) / Lay Odds
# This guarantees equal profit across all outcomes once the Lay order is matched.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# FORMULA 2: Lay Liability Check (Phase 2)
# Liability = Lay Stake * (Lay Odds - 1)
# Must satisfy: Liability <= Available Bankroll (free_funds) before placing Lay.
# ---------------------------------------------------------------------------


def _green_up_lay_stake(back_stake: float, back_odds: float, lay_odds: float) -> float:
    """
    Compute the Lay stake required to Green Up (equal profit all outcomes).
    Formula: Lay Stake = (Back Stake * Back Odds) / Lay Odds
    """
    if lay_odds <= 0:
        return 0.0
    return round((back_stake * back_odds) / lay_odds, 2)


def _lay_liability(lay_stake: float, lay_odds: float) -> float:
    """
    Compute Lay liability. Must be <= free_funds before placing Lay.
    Formula: Liability = Lay Stake * (Lay Odds - 1)
    """
    return lay_stake * (lay_odds - 1)


def _get_best_back_lay(prices: list[dict]) -> tuple[Optional[float], Optional[float]]:
    """
    Extract best Back and best Lay odds from runner prices.
    Best Back = highest odds (best price for us when backing)
    Best Lay = lowest odds (best price for us when laying)
    """
    best_back = None
    best_lay = None
    for p in prices or []:
        side = p.get("side", "").lower()
        odds = p.get("odds") or p.get("decimal-odds")
        if odds is None:
            continue
        if side == "back":
            if best_back is None or odds > best_back:
                best_back = odds
        elif side == "lay":
            if best_lay is None or odds < best_lay:
                best_lay = odds
    return best_back, best_lay


def _round_odds(odds: float) -> float:
    """Round odds to tick size."""
    tick = config.TICK_SIZE
    return round(round(odds / tick) * tick, 2)


async def _hedge_with_retry(
    api: MatchbookAPI,
    runner_id: int,
    back_stake: float,
    back_odds: float,
    market_name: str,
    runner_name: str,
) -> bool:
    """
    Place Green Up Lay order, retrying on Market Suspended every 2 seconds.
    Recalculates lay_odds from live prices on each retry to cut losses if market moved.
    """
    for attempt in range(config.MAX_HEDGE_RETRIES):
        try:
            lay_odds = await _fetch_lay_odds(api, runner_id)
            if lay_odds is None or lay_odds <= 0:
                logger.warning("No valid lay odds for hedge, retrying...")
                await asyncio.sleep(config.HEDGE_RETRY_INTERVAL_SEC)
                continue

            lay_stake = _green_up_lay_stake(back_stake, back_odds, lay_odds)
            if lay_stake <= 0:
                logger.warning("Invalid green up stake")
                return False

            offers = [
                {
                    "runner-id": runner_id,
                    "side": "lay",
                    "odds": _round_odds(lay_odds),
                    "stake": lay_stake,
                    "keep-in-play": False,
                }
            ]
            result = await api.submit_offers(offers)
            if result and result[0].get("status") in ("open", "matched"):
                logger.info(
                    "Hedge placed: Lay %.2f @ %.2f (Green Up) for %s",
                    lay_stake,
                    lay_odds,
                    runner_name,
                )
                db.insert_trade(
                    market_id=0,
                    runner_id=runner_id,
                    market_name=market_name,
                    runner_name=runner_name,
                    side="lay",
                    odds=lay_odds,
                    stake=lay_stake,
                    status=result[0].get("status", "open"),
                    offer_id=result[0].get("id"),
                    phase=1,
                )
                return True
        except MarketSuspendedError:
            logger.warning(
                "Market suspended during hedge (attempt %d/%d), retrying in %ds",
                attempt + 1,
                config.MAX_HEDGE_RETRIES,
                config.HEDGE_RETRY_INTERVAL_SEC,
            )
            await asyncio.sleep(config.HEDGE_RETRY_INTERVAL_SEC)
        except Exception as e:
            logger.exception("Hedge failed: %s", e)
            await asyncio.sleep(config.HEDGE_RETRY_INTERVAL_SEC)
    return False


def _get_sport_ids():
    """Sport IDs from DB settings or config."""
    return db.get_sport_ids()


def _get_market_types():
    """Market types from DB settings or config."""
    return db.get_market_types()


def _parse_event_start(start_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO 8601 event start string to datetime (UTC)."""
    if not start_str:
        return None
    try:
        s = str(start_str).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


async def _close_events_before_start(api: MatchbookAPI) -> None:
    """
    When pre-match only: cancel open orders and hedge matched positions
    for events starting within close_before_start_minutes.
    """
    if not db.get_pre_match_only():
        return

    offers = await api.get_offers(statuses=["open", "matched"])
    if not offers:
        return

    event_ids = list({o.get("event-id") for o in offers if o.get("event-id")})
    if not event_ids:
        return

    events = await api.get_events(
        event_ids=event_ids,
        include_prices=False,
        pre_match_only=False,
    )
    close_minutes = db.get_close_before_start_minutes()
    now = datetime.now(timezone.utc)
    threshold = now.timestamp() + (close_minutes * 60)

    events_to_close = []
    for ev in events:
        start = _parse_event_start(ev.get("start"))
        if start and start.timestamp() <= threshold:
            events_to_close.append(ev.get("id"))

    if not events_to_close:
        return

    for event_id in events_to_close:
        event_offers = [o for o in offers if o.get("event-id") == event_id]
        if not event_offers:
            continue

        event_name = event_offers[0].get("event-name", event_id)
        logger.info("Closing orders for event %s (starts within %d min)", event_name, int(close_minutes))

        # Cancel all open offers for this event
        if not db.get_paper_trading():
            try:
                await api.cancel_offers(event_ids=[event_id])
                logger.info("Cancelled open offers for event %s", event_id)
            except Exception as e:
                logger.exception("Failed to cancel offers: %s", e)

        # Hedge matched Back positions (Green Up)
        for o in event_offers:
            if o.get("status") != "matched" or o.get("side") != "back":
                continue
            back_stake = float(o.get("stake", 0) or o.get("remaining", 0))
            back_odds = float(o.get("odds", 0) or o.get("decimal-odds", 0))
            runner_id = o.get("runner-id")
            if back_stake <= 0 or back_odds <= 0 or not runner_id:
                continue
            if not db.get_paper_trading():
                await _hedge_with_retry(
                    api,
                    runner_id,
                    back_stake,
                    back_odds,
                    o.get("market-name", ""),
                    o.get("runner-name", ""),
                )
            await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)


async def _run_phase1(api: MatchbookAPI) -> None:
    """
    Phase 1: Directional Scalping ("Buy the Dip").
    Bankroll £25–£200. Only place Back orders at a discount (2 ticks above best).
    On match: immediately Green Up with Lay order.
    """
    logger.info("Running Phase 1 (Scalping)")
    sport_ids = _get_sport_ids()
    market_types = _get_market_types()
    events = await api.get_events(
        sport_ids=sport_ids,
        include_prices=True,
        price_depth=3,
        states="open",
        per_page=50,
    )

    # Build list of (event, market, runner) with valid prices
    candidates = []
    for event in events:
        for market in event.get("markets", []):
            if market.get("market-type") not in market_types:
                continue
            if market.get("status") != "open":
                continue
            for runner in market.get("runners", []):
                if runner.get("status") != "open":
                    continue
                prices = runner.get("prices", [])
                best_back, best_lay = _get_best_back_lay(prices)
                if best_back is None or best_lay is None:
                    continue
                candidates.append(
                    (event, market, runner, best_back, best_lay)
                )

    account = api.get_account()
    free_funds = float(account.get("free-funds", 0) or 0)
    # Phase 1: use small stake, max ~10% of bankroll per order
    max_stake = min(free_funds * 0.1, 5.0)
    if max_stake < 2.0:
        logger.info("Insufficient funds for Phase 1 (need >= £2)")
        return

    for event, market, runner, best_back, best_lay in candidates[:5]:
        # Place Back at best_back + (TICK_SIZE * BACK_TICKS_ABOVE)
        back_odds = _round_odds(best_back + config.TICK_SIZE * config.BACK_TICKS_ABOVE)
        stake = round(max_stake, 2)

        try:
            if db.get_paper_trading():
                db.insert_paper_trade(
                    event_name=event.get("name", ""),
                    market_name=market.get("name", ""),
                    runner_name=runner.get("name", ""),
                    side="back",
                    odds=back_odds,
                    stake=stake,
                    phase=1,
                    reason="Phase 1: Back at discount (2 ticks above best)",
                )
                logger.info(
                    "PAPER: Phase 1 Back would place: %s @ %.2f x %.2f",
                    runner.get("name"),
                    back_odds,
                    stake,
                )
            else:
                offers = [
                    {
                        "runner-id": runner["id"],
                        "side": "back",
                        "odds": back_odds,
                        "stake": stake,
                        "keep-in-play": False,
                    }
                ]
                result = await api.submit_offers(offers)
                if result:
                    offer = result[0]
                    db.insert_trade(
                        market_id=market.get("id"),
                        runner_id=runner["id"],
                        market_name=market.get("name", ""),
                        runner_name=runner.get("name", ""),
                        side="back",
                        odds=back_odds,
                        stake=stake,
                        status=offer.get("status", "open"),
                        offer_id=offer.get("id"),
                        phase=1,
                    )
                    db.insert_position(
                        market_id=market.get("id"),
                        runner_id=runner["id"],
                        market_name=market.get("name", ""),
                        runner_name=runner.get("name", ""),
                        side="back",
                        entry_odds=back_odds,
                        entry_stake=stake,
                        offer_id=offer.get("id"),
                    )
                    logger.info(
                        "Phase 1 Back placed: %s @ %.2f x %.2f",
                        runner.get("name"),
                        back_odds,
                        stake,
                    )
        except MarketSuspendedError:
            logger.warning("Market suspended, skipping")
        except Exception as e:
            logger.exception("Phase 1 order failed: %s", e)

        await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)

    # Poll for matched offers and hedge (Green Up)
    open_offers = await api.get_offers(statuses=["open", "matched"])
    for offer in open_offers:
        if offer.get("side") != "back":
            continue
        if offer.get("status") == "matched":
            back_stake = float(offer.get("stake", 0) or offer.get("remaining", 0))
            back_odds = float(offer.get("odds", 0) or offer.get("decimal-odds", 0))
            runner_id = offer.get("runner-id")
            market_name = offer.get("market-name", "")
            runner_name = offer.get("runner-name", "")
            if back_stake <= 0 or back_odds <= 0:
                continue
            await _hedge_with_retry(
                api,
                runner_id,
                back_stake,
                back_odds,
                market_name,
                runner_name,
            )
            break  # Process one at a time


async def _fetch_lay_odds(api: MatchbookAPI, runner_id: int) -> Optional[float]:
    """Fetch current best Lay odds for a runner from events."""
    events = await api.get_events(
        sport_ids=_get_sport_ids(),
        include_prices=True,
        price_depth=1,
        states="open",
        per_page=100,
    )
    for event in events:
        for market in event.get("markets", []):
            for runner in market.get("runners", []):
                if runner.get("id") == runner_id:
                    _, best_lay = _get_best_back_lay(runner.get("prices", []))
                    return best_lay
    return None


async def _run_phase2(api: MatchbookAPI) -> None:
    """
    Phase 2: Market Making / Spread Harvesting.
    Bankroll >= £200. Place Back and Lay simultaneously at spread edges.
    Liability check: Lay Stake * (Lay Odds - 1) <= free_funds.
    """
    logger.info("Running Phase 2 (Market Making)")
    account = api.get_account()
    free_funds = float(account.get("free-funds", 0) or 0)
    sport_ids = _get_sport_ids()
    market_types = _get_market_types()

    events = await api.get_events(
        sport_ids=sport_ids,
        include_prices=True,
        price_depth=3,
        states="open",
        per_page=50,
    )

    for event in events:
        for market in event.get("markets", []):
            if market.get("market-type") not in market_types:
                continue
            if market.get("status") != "open":
                continue
            for runner in market.get("runners", []):
                if runner.get("status") != "open":
                    continue
                prices = runner.get("prices", [])
                best_back, best_lay = _get_best_back_lay(prices)
                if best_back is None or best_lay is None:
                    continue

                back_odds = _round_odds(
                    best_back + config.TICK_SIZE * config.PHASE2_BACK_TICKS_ABOVE
                )
                lay_odds = _round_odds(
                    best_lay - config.TICK_SIZE * config.PHASE2_LAY_TICKS_BELOW
                )
                if lay_odds <= back_odds:
                    continue  # No spread to harvest

                stake = min(free_funds * 0.05, 10.0)
                stake = round(stake, 2)
                if stake < 2.0:
                    continue

                # Liability check: Lay Stake * (Lay Odds - 1) <= free_funds
                liability = _lay_liability(stake, lay_odds)
                if liability > free_funds:
                    logger.debug("Liability %.2f exceeds free_funds %.2f, skipping", liability, free_funds)
                    continue

                try:
                    if db.get_paper_trading():
                        db.insert_paper_trade(
                            event_name=event.get("name", ""),
                            market_name=market.get("name", ""),
                            runner_name=runner.get("name", ""),
                            side="back",
                            odds=back_odds,
                            stake=stake,
                            phase=2,
                            reason="Phase 2: Back at spread edge",
                        )
                        db.insert_paper_trade(
                            event_name=event.get("name", ""),
                            market_name=market.get("name", ""),
                            runner_name=runner.get("name", ""),
                            side="lay",
                            odds=lay_odds,
                            stake=stake,
                            phase=2,
                            reason="Phase 2: Lay at spread edge",
                        )
                        logger.info(
                            "PAPER: Phase 2 would place Back %.2f Lay %.2f @ %.2f for %s",
                            back_odds,
                            lay_odds,
                            stake,
                            runner.get("name"),
                        )
                    else:
                        offers = [
                        {
                            "runner-id": runner["id"],
                            "side": "back",
                            "odds": back_odds,
                            "stake": stake,
                            "keep-in-play": False,
                        },
                        {
                            "runner-id": runner["id"],
                            "side": "lay",
                            "odds": lay_odds,
                            "stake": stake,
                            "keep-in-play": False,
                        },
                        ]
                        result = await api.submit_offers(offers)
                        if result:
                            logger.info(
                                "Phase 2 orders placed: %s Back %.2f Lay %.2f @ %.2f",
                                runner.get("name"),
                                back_odds,
                                lay_odds,
                                stake,
                            )
                except MarketSuspendedError:
                    logger.warning("Market suspended")
                except Exception as e:
                    logger.exception("Phase 2 order failed: %s", e)

                await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)
                return  # One market per cycle


async def _main_loop() -> None:
    """Main bot loop: snapshot bankroll, run phase logic, rate limit."""
    db.init_db()
    trading_enabled = db.get_bot_enabled()

    api = MatchbookAPI()
    try:
        await api.ensure_auth()
        account = api.get_account()
        balance = float(account.get("balance", 0) or 0)
        exposure = float(account.get("exposure", 0) or 0)
        free_funds = float(account.get("free-funds", 0) or 0)

        daily_roi = db.get_daily_roi_pct()
        db.insert_bankroll_snapshot(balance, exposure, free_funds, daily_roi)

        if not trading_enabled:
            logger.info("Bot is paused. Snapshot recorded, no orders placed.")
            return

        # Pre-match only: close orders for events starting soon
        await _close_events_before_start(api)

        # Daily stop-loss: pause if today's loss exceeds limit
        if db.get_stop_loss_triggered():
            logger.warning("Daily stop-loss triggered. Trading paused until tomorrow or manual clear.")
            return

        start_balance = db.get_daily_start_balance()
        stop_loss_pct = db.get_daily_stop_loss_pct()
        if start_balance and start_balance > 0:
            daily_loss_pct = ((start_balance - balance) / start_balance) * 100
            if daily_loss_pct >= stop_loss_pct:
                db.set_stop_loss_triggered()
                logger.warning(
                    "Daily stop-loss triggered: %.1f%% loss (limit %.1f%%). Trading paused.",
                    daily_loss_pct,
                    stop_loss_pct,
                )
                return

        phase = 2 if free_funds >= config.PHASE2_MIN_BANKROLL else 1
        logger.info("Bankroll: £%.2f, Phase: %d", free_funds, phase)

        if phase == 1:
            await _run_phase1(api)
        else:
            await _run_phase2(api)

    finally:
        await api.close()


def main() -> None:
    """Entry point for the bot."""
    asyncio.run(_main_loop())


if __name__ == "__main__":
    main()
