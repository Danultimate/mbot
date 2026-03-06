"""
Matchbook Automated Trading Bot.
Background async process running Phase 1 (Scalping) and Phase 2 (Market Making) logic.
Implements Green Up formula, Lay liability check, and market suspension retry.
"""

import asyncio
import logging
import traceback
from datetime import datetime, timezone
from typing import Optional

import alerts
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
    Note: Matchbook applies commission (2% on winnings) at settlement; balance is already net.
    """
    if lay_odds <= 0:
        return 0.0
    return round((back_stake * back_odds) / lay_odds, 2)


def _green_up_back_stake(lay_stake: float, lay_odds: float, back_odds: float) -> float:
    """
    Compute the Back stake required to Green Up a Lay position (Lay-First exit).
    Formula: Back Stake = (Lay Stake * Lay Odds) / Back Odds
    """
    if back_odds <= 0:
        return 0.0
    return round((lay_stake * lay_odds) / back_odds, 2)


def _net_green_up_profit(lay_stake: float, back_stake: float) -> float:
    """
    Net profit for perfectly hedged trade.
    Formula: Net Profit = Matched Lay Stake - Matched Back Stake.
    Works for both Back-then-Lay and Lay-then-Back cycles.
    """
    return round(float(lay_stake) - float(back_stake), 2)


def _locked_in_profit_back_hedge(
    back_stake: float, back_odds: float, lay_odds: float
) -> float:
    """Locked-in profit when hedging a Back with Lay. Rounded to 2 dp."""
    if lay_odds <= 0:
        return 0.0
    return round(back_stake * (back_odds / lay_odds - 1), 2)


def _locked_in_profit_lay_hedge(
    lay_stake: float, lay_odds: float, back_odds: float
) -> float:
    """Locked-in profit when hedging a Lay with Back. Rounded to 2 dp."""
    if back_odds <= 0:
        return 0.0
    return round(lay_stake * (back_odds - lay_odds) / back_odds, 2)


def _lay_liability(lay_stake: float, lay_odds: float) -> float:
    """
    Compute Lay liability. Must be <= free_funds before placing Lay.
    Formula: Liability = Lay Stake * (Lay Odds - 1)
    """
    return lay_stake * (lay_odds - 1)


def _passes_liquidity_filter(event: dict, market: dict) -> bool:
    """Strict liquidity filter. Uses config thresholds and optional ALLOWED_CATEGORY_IDS."""
    return MatchbookAPI.passes_liquidity_filter(
        event,
        market,
        config.MIN_EVENT_VOLUME,
        config.MIN_MARKET_VOLUME,
        getattr(config, "ALLOWED_CATEGORY_IDS", None) or [],
    )


def _can_enter_selection(
    market_id: int,
    runner_id: int,
    exposed_runners: set[tuple[int, int]],
) -> bool:
    """
    Strict pre-entry check: zero open/unmatched positions for this market/selection.
    Returns False if we have exposure or are on cooldown.
    """
    if db.is_market_blacklisted(market_id):
        logger.debug("Skip %s/%s: market blacklisted (Lay exit)", market_id, runner_id)
        return False
    if db.has_open_position_for_runner(market_id, runner_id):
        logger.debug("Skip %s/%s: open position", market_id, runner_id)
        return False
    if db.is_on_cooldown(market_id, runner_id, config.ENTRY_COOLDOWN_SEC):
        logger.debug("Skip %s/%s: cooldown", market_id, runner_id)
        return False
    if (market_id, runner_id) in exposed_runners:
        logger.debug("Skip %s/%s: API has open/matched offer", market_id, runner_id)
        return False
    return True


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
    market_id: int = 0,
    event_id: Optional[int] = None,
    event_name: str = "",
    back_offer_id: Optional[int] = None,
    emergency_close: bool = False,
) -> tuple[bool, Optional[float]]:
    """
    Place Green Up Lay order, retrying on Market Suspended every 2 seconds.
    Maker by default: Lay at best_lay - ticks (unmatched). Time Stop uses emergency_close=True to cross.
    Returns (success, locked_in_profit).
    """
    for attempt in range(config.MAX_HEDGE_RETRIES):
        try:
            best_lay = await _fetch_lay_odds(api, runner_id)
            if best_lay is None or best_lay <= 0:
                logger.warning("No valid lay odds for hedge, retrying...")
                await asyncio.sleep(config.HEDGE_RETRY_INTERVAL_SEC)
                continue

            # Maker: place at best_lay - ticks (or lower). Emergency: place at best_lay (take).
            if emergency_close:
                lay_odds = best_lay
            else:
                ticks_below = max(1, config.HEDGE_LAY_TICKS_BELOW)
                lay_odds = _round_odds(best_lay - config.TICK_SIZE * ticks_below)
                if lay_odds < 1.02:  # Avoid invalid odds on short prices
                    lay_odds = best_lay

            lay_stake = _green_up_lay_stake(back_stake, back_odds, lay_odds)
            if lay_stake <= 0:
                logger.warning("Invalid green up stake")
                return False, None

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
                profit = _locked_in_profit_back_hedge(back_stake, back_odds, lay_odds)
                logger.info(
                    "Hedge placed: Lay %.2f @ %.2f (Green Up) for %s, profit £%.2f",
                    lay_stake,
                    lay_odds,
                    runner_name,
                    profit,
                )
                if not db.get_paper_trading():
                    # Rule 2 (Source of Truth): do NOT log complete on execution. Poll API first.
                    pos = db.get_position_by_offer_id(back_offer_id) if back_offer_id else None
                    db.insert_pending_hedge_confirmation(
                        hedge_offer_id=result[0].get("id"),
                        market_id=market_id,
                        runner_id=runner_id,
                        side="lay",
                        stake=lay_stake,
                        odds=lay_odds,
                        market_name=market_name,
                        runner_name=runner_name,
                        event_id=event_id or 0,
                        position_id=pos["id"] if pos else None,
                        back_offer_id=back_offer_id,
                        event_name=event_name,
                    )
                    # Hard Lock: hedge_pending + blacklist set by caller BEFORE this call
                return True, profit
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
    asyncio.to_thread(
        alerts.send_alert,
        f"Hedge failed for {runner_name} after {config.MAX_HEDGE_RETRIES} retries.",
        "hedge_failed",
    )
    return False, None


async def _fetch_back_odds(api: MatchbookAPI, runner_id: int) -> Optional[float]:
    """Fetch current best Back odds for a runner from events."""
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
                    best_back, _ = _get_best_back_lay(runner.get("prices", []))
                    return best_back
    return None


async def _hedge_lay_with_retry(
    api: MatchbookAPI,
    runner_id: int,
    lay_stake: float,
    lay_odds: float,
    market_name: str,
    runner_name: str,
    market_id: int = 0,
    event_id: Optional[int] = None,
    event_name: str = "",
    lay_offer_id: Optional[int] = None,
    emergency_close: bool = False,
) -> tuple[bool, Optional[float]]:
    """
    Place Green Up Back order to close a Lay position.
    Maker by default: Back at best_back - ticks (unmatched). Time Stop uses emergency_close=True to cross.
    Returns (success, locked_in_profit).
    """
    for attempt in range(config.MAX_HEDGE_RETRIES):
        try:
            best_back = await _fetch_back_odds(api, runner_id)
            if best_back is None or best_back <= 0:
                logger.warning("No valid back odds for Lay hedge, retrying...")
                await asyncio.sleep(config.HEDGE_RETRY_INTERVAL_SEC)
                continue

            # Maker: place at best_back - ticks (or lower). Emergency: place at best_back (take).
            if emergency_close:
                back_odds = best_back
            else:
                ticks_below = max(1, config.HEDGE_BACK_TICKS_BELOW)
                back_odds = _round_odds(best_back - config.TICK_SIZE * ticks_below)
                if back_odds < 1.02:  # Avoid invalid odds on short prices
                    back_odds = best_back

            back_stake = _green_up_back_stake(lay_stake, lay_odds, back_odds)
            if back_stake <= 0:
                logger.warning("Invalid green up back stake")
                return False, None

            offers = [
                {
                    "runner-id": runner_id,
                    "side": "back",
                    "odds": _round_odds(back_odds),
                    "stake": back_stake,
                    "keep-in-play": False,
                }
            ]
            result = await api.submit_offers(offers)
            if result and result[0].get("status") in ("open", "matched"):
                profit = _locked_in_profit_lay_hedge(lay_stake, lay_odds, back_odds)
                logger.info(
                    "Lay hedge placed: Back %.2f @ %.2f (Green Up) for %s, profit £%.2f",
                    back_stake,
                    back_odds,
                    runner_name,
                    profit,
                )
                if not db.get_paper_trading():
                    # Rule 2 (Source of Truth): do NOT log complete on execution. Poll API first.
                    pos = db.get_position_by_offer_id(lay_offer_id) if lay_offer_id else None
                    db.insert_pending_hedge_confirmation(
                        hedge_offer_id=result[0].get("id"),
                        market_id=market_id,
                        runner_id=runner_id,
                        side="back",
                        stake=back_stake,
                        odds=back_odds,
                        market_name=market_name,
                        runner_name=runner_name,
                        event_id=event_id or 0,
                        position_id=pos["id"] if pos else None,
                        back_offer_id=lay_offer_id,
                        event_name=event_name,
                    )
                    # Hard Lock: hedge_pending + blacklist set by caller BEFORE this call
                return True, profit
        except MarketSuspendedError:
            logger.warning(
                "Market suspended during Lay hedge (attempt %d/%d), retrying in %ds",
                attempt + 1,
                config.MAX_HEDGE_RETRIES,
                config.HEDGE_RETRY_INTERVAL_SEC,
            )
            await asyncio.sleep(config.HEDGE_RETRY_INTERVAL_SEC)
        except Exception as e:
            logger.exception("Lay hedge failed: %s", e)
            await asyncio.sleep(config.HEDGE_RETRY_INTERVAL_SEC)
    asyncio.to_thread(
        alerts.send_alert,
        f"Lay hedge failed for {runner_name} after {config.MAX_HEDGE_RETRIES} retries.",
        "hedge_failed",
    )
    return False, None


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


async def _cancel_high_odds_orders(api: MatchbookAPI) -> None:
    """
    Cancel open/unmatched orders where odds > MAX_ODDS_CANCEL (4.50).
    These are dead trades (massive underdogs, poor scalping).
    """
    if db.get_paper_trading():
        return
    offers = await api.get_offers(statuses=["open"])
    to_cancel = []
    for o in offers:
        if o.get("status") != "open":
            continue
        odds = float(o.get("odds", 0) or o.get("decimal-odds", 0) or 0)
        if odds > config.MAX_ODDS_CANCEL:
            to_cancel.append(o.get("id"))
    if to_cancel:
        try:
            await api.cancel_offers(offer_ids=to_cancel)
            logger.info("Cancelled %d high-odds orders (odds > %.2f)", len(to_cancel), config.MAX_ODDS_CANCEL)
        except Exception as e:
            logger.warning("Failed to cancel high-odds orders: %s", e)


async def _cancel_low_volume_orders(api: MatchbookAPI) -> None:
    """
    Cancel any OPEN orders in markets with volume < LOW_VOLUME_CANCEL_THRESHOLD (£1k).
    Frees bankroll stuck in obscure/low-liquidity markets.
    """
    if db.get_paper_trading():
        return
    offers = await api.get_offers(statuses=["open"])
    open_offers = [o for o in offers if o.get("status") == "open"]
    if not open_offers:
        return
    event_ids = list({o.get("event-id") for o in open_offers if o.get("event-id")})
    if not event_ids:
        return
    try:
        events = await api.get_events(event_ids=event_ids, include_prices=False)
    except Exception as e:
        logger.warning("Could not fetch events for low-volume cancel: %s", e)
        return
    market_volume: dict[int, float] = {}
    for ev in events:
        for m in ev.get("markets", []):
            mid = m.get("id")
            if mid is not None:
                market_volume[int(mid)] = float(m.get("volume", 0) or 0)
    to_cancel = []
    for o in open_offers:
        mid = o.get("market-id")
        if mid is None:
            continue
        vol = market_volume.get(int(mid), 0)
        if vol < config.LOW_VOLUME_CANCEL_THRESHOLD:
            to_cancel.append(o.get("id"))
    if to_cancel:
        try:
            await api.cancel_offers(offer_ids=to_cancel)
            logger.info("Cancelled %d orders in low-volume markets (<£%.0f)", len(to_cancel), config.LOW_VOLUME_CANCEL_THRESHOLD)
        except Exception as e:
            logger.warning("Failed to cancel low-volume orders: %s", e)


async def _run_startup_state_recovery(api: MatchbookAPI) -> None:
    """
    Startup State Recovery: adopt orphaned exchange orders into local tracking.
    Run on init before any new trades. Query API for open+matched orders, compare to local state.
    Orphaned Phase 1 Lays are adopted (position). Orphaned Lay+Back pairs adopted (position + pending_hedge).
    Orphaned lone Back (naked long) cannot be adopted: fire Taker Lay hedge immediately.
    If adoption fails (missing data), fire Taker hedge to close exposure.
    """
    if db.get_paper_trading():
        return
    try:
        offers = await api.get_offers(statuses=["open", "matched", "settled"])
    except Exception as e:
        logger.warning("Startup state recovery: could not fetch offers: %s", e)
        return
    if not offers:
        return
    tracked = db.get_all_tracked_offer_ids()
    orphans = [o for o in offers if o.get("id") is not None and int(o.get("id")) not in tracked]
    if not orphans:
        return
    logger.info("Startup state recovery: found %d orphaned order(s) on exchange", len(orphans))
    by_runner: dict[tuple[int, int], list[dict]] = {}
    for o in orphans:
        mid = o.get("market-id")
        rid = o.get("runner-id")
        if mid is not None and rid is not None:
            key = (int(mid), int(rid))
            by_runner.setdefault(key, []).append(o)
    for key, runner_offers in by_runner.items():
        market_id, runner_id = key
        backs = [o for o in runner_offers if (o.get("side") or "").lower() == "back"]
        lays = [o for o in runner_offers if (o.get("side") or "").lower() == "lay"]
        lay_o = lays[0] if lays else None
        back_o = backs[0] if backs else None

        def _valid(o: dict) -> bool:
            s = float(o.get("stake", 0) or o.get("remaining", 0) or 0)
            od = float(o.get("odds", 0) or o.get("decimal-odds", 0) or 0)
            return s > 0 and od > 0

        if lay_o and back_o:
            if not _valid(lay_o) or not _valid(back_o):
                logger.warning("Recovery: orphan Lay+Back for %s/%s has invalid stake/odds, firing taker hedge for Lay", market_id, runner_id)
                stake = float(lay_o.get("stake", 0) or lay_o.get("remaining", 0) or 0)
                odds = float(lay_o.get("odds", 0) or lay_o.get("decimal-odds", 0) or 0)
                if stake > 0 and odds > 0:
                    db.insert_hedge_initiated(lay_o.get("id"))
                    ok, _ = await _hedge_lay_with_retry(
                        api, runner_id, stake, odds,
                        lay_o.get("market-name", ""), lay_o.get("runner-name", ""),
                        market_id=market_id, event_id=lay_o.get("event-id") or 0,
                        event_name=lay_o.get("event-name", ""), lay_offer_id=lay_o.get("id"),
                        emergency_close=True,
                    )
                    if ok:
                        logger.info("Recovery: taker Back hedge placed for Lay %s", lay_o.get("runner-name", ""))
                continue
            pos = db.insert_position(
                market_id=market_id,
                runner_id=runner_id,
                market_name=lay_o.get("market-name", ""),
                runner_name=lay_o.get("runner-name", ""),
                side="lay",
                entry_odds=float(lay_o.get("odds", 0) or lay_o.get("decimal-odds", 0)),
                entry_stake=float(lay_o.get("stake", 0) or lay_o.get("remaining", 0)),
                offer_id=lay_o.get("id"),
            )
            db.insert_hedge_initiated(lay_o.get("id"))
            stake_b = float(back_o.get("stake", 0) or back_o.get("remaining", 0))
            odds_b = float(back_o.get("odds", 0) or back_o.get("decimal-odds", 0))
            db.insert_pending_hedge_confirmation(
                hedge_offer_id=back_o.get("id"),
                market_id=market_id,
                runner_id=runner_id,
                side="back",
                stake=stake_b,
                odds=odds_b,
                market_name=back_o.get("market-name", ""),
                runner_name=back_o.get("runner-name", ""),
                event_id=back_o.get("event-id") or 0,
                position_id=pos,
                back_offer_id=lay_o.get("id"),
                event_name=back_o.get("event-name", ""),
            )
            db.update_position_to_hedge_pending(pos)
            db.insert_blacklisted_market(market_id, lay_o.get("event-id") or 0)
            logger.info("Recovery: adopted Lay+Back for %s (market %s)", lay_o.get("runner-name", ""), market_id)
            await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)
            continue

        if lay_o:
            if not _valid(lay_o):
                logger.warning("Recovery: orphan Lay for %s/%s has invalid stake/odds, firing taker hedge", market_id, runner_id)
                stake = float(lay_o.get("stake", 0) or lay_o.get("remaining", 0) or 0)
                odds = float(lay_o.get("odds", 0) or lay_o.get("decimal-odds", 0) or 0)
                if stake > 0 and odds > 0:
                    db.insert_hedge_initiated(lay_o.get("id"))
                    ok, _ = await _hedge_lay_with_retry(
                        api, runner_id, stake, odds,
                        lay_o.get("market-name", ""), lay_o.get("runner-name", ""),
                        market_id=market_id, event_id=lay_o.get("event-id") or 0,
                        event_name=lay_o.get("event-name", ""), lay_offer_id=lay_o.get("id"),
                        emergency_close=True,
                    )
                    if ok:
                        logger.info("Recovery: taker Back hedge placed for orphan Lay %s", lay_o.get("runner-name", ""))
                continue
            db.insert_position(
                market_id=market_id,
                runner_id=runner_id,
                market_name=lay_o.get("market-name", ""),
                runner_name=lay_o.get("runner-name", ""),
                side="lay",
                entry_odds=float(lay_o.get("odds", 0) or lay_o.get("decimal-odds", 0)),
                entry_stake=float(lay_o.get("stake", 0) or lay_o.get("remaining", 0)),
                offer_id=lay_o.get("id"),
            )
            logger.info("Recovery: adopted orphan Lay for %s (market %s) - Stop-Loss will manage", lay_o.get("runner-name", ""), market_id)
            await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)
            continue

        if back_o:
            logger.warning("Recovery: lone orphan Back (naked long) for %s - firing taker Lay hedge", back_o.get("runner-name", ""))
            stake = float(back_o.get("stake", 0) or back_o.get("remaining", 0) or 0)
            odds = float(back_o.get("odds", 0) or back_o.get("decimal-odds", 0) or 0)
            if stake <= 0 or odds <= 0:
                logger.error("Recovery: cannot hedge lone Back - invalid stake/odds")
                continue
            db.insert_hedge_initiated(back_o.get("id"))
            ok, _ = await _hedge_with_retry(
                api, runner_id, stake, odds,
                back_o.get("market-name", ""), back_o.get("runner-name", ""),
                market_id=market_id, event_id=back_o.get("event-id") or 0,
                event_name=back_o.get("event-name", ""), back_offer_id=back_o.get("id"),
                emergency_close=True,
            )
            if ok:
                logger.info("Recovery: taker Lay hedge placed for lone Back %s", back_o.get("runner-name", ""))
            await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)


async def _close_events_before_start(api: MatchbookAPI) -> bool:
    """
    When pre-match only: cancel open orders and hedge matched positions
    for events starting within close_before_start_minutes.
    Returns True if any hedge order was placed (for API latency buffer).
    """
    if not db.get_pre_match_only():
        return False

    offers = await api.get_offers(statuses=["open", "matched"])
    if not offers:
        return False

    event_ids = list({o.get("event-id") for o in offers if o.get("event-id")})
    if not event_ids:
        return False

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
        return False

    pending = db.get_pending_hedge_confirmations()
    child_offer_ids = {p["hedge_offer_id"] for p in pending}
    parent_ids_already_hedged = db.get_hedge_initiated_parent_ids()
    parent_ids_already_hedged.update(p["back_offer_id"] for p in pending if p.get("back_offer_id"))
    order_placed = False
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

        # Hedge matched positions - Hard Lock: Execution Block + mark BEFORE API call
        for o in event_offers:
            if o.get("status") != "matched":
                continue
            parent_id = o.get("id")
            if parent_id in child_offer_ids:
                continue
            if parent_id in parent_ids_already_hedged:
                continue
            pos = db.get_position_by_offer_id(parent_id)
            if pos and pos.get("status") == "hedge_pending":
                continue
            market_id = int(o.get("market-id") or 0)
            if db.is_market_blacklisted(market_id):
                continue
            runner_id = o.get("runner-id")
            stake = float(o.get("stake", 0) or o.get("remaining", 0))
            odds = float(o.get("odds", 0) or o.get("decimal-odds", 0))
            if stake <= 0 or odds <= 0 or not runner_id:
                continue
            if o.get("side") == "back":
                if not db.get_paper_trading():
                    db.insert_hedge_initiated(parent_id)
                    if pos:
                        db.update_position_to_hedge_pending(pos["id"])
                    db.insert_blacklisted_market(market_id, event_id)
                    ok, _ = await _hedge_with_retry(
                        api, runner_id, stake, odds,
                        o.get("market-name", ""), o.get("runner-name", ""),
                        market_id=market_id, event_id=event_id,
                        event_name=o.get("event-name", ""),
                        back_offer_id=o.get("id"),
                        emergency_close=True,  # Time Stop: cross spread for immediate exit
                    )
                    if ok:
                        order_placed = True
            elif o.get("side") == "lay":
                if not db.get_paper_trading():
                    db.insert_hedge_initiated(parent_id)
                    if pos:
                        db.update_position_to_hedge_pending(pos["id"])
                    db.insert_blacklisted_market(market_id, event_id)
                    ok, _ = await _hedge_lay_with_retry(
                        api, runner_id, stake, odds,
                        o.get("market-name", ""), o.get("runner-name", ""),
                        market_id=market_id, event_id=event_id,
                        event_name=o.get("event-name", ""),
                        lay_offer_id=o.get("id"),
                        emergency_close=True,
                    )
                    if ok:
                        order_placed = True
            await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)
    return order_placed


async def _run_phase1(api: MatchbookAPI) -> bool:
    """
    Phase 1: Lay-First Strategy ("Lay the Rise").
    Bankroll £25–£200. Place Maker Lay orders 2 ticks below best Lay.
    On match: hedge with Maker Back order (Phase 2).
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

    db.insert_api_log(
        "response", "BOT", "get_events", None,
        request_body=f"Phase 1: {len(events)} events (sport_ids={sport_ids}). "
        f"Liquidity: min £{config.MIN_EVENT_VOLUME:,} event / £{config.MIN_MARKET_VOLUME:,} market.",
    )

    # Build list of (event, market, runner) with valid prices
    # Matchbook may use one_x_two, one-x-two, money_line, over_under_25, over-under-2-5, etc.
    def _norm(s: str) -> str:
        return (s or "").lower().replace("-", "_").replace(".", "_").replace(" ", "_")

    # Aliases: Matchbook slugs can vary (e.g. over-under-2-5 vs over_under_25)
    def _canonical(s: str) -> str:
        n = _norm(s)
        if n in ("over_under_2_5", "over_under_25"):
            return "over_under_25"
        return n

    def _market_matches(mt: str) -> bool:
        if not mt:
            return False
        mt_c = _canonical(mt)
        for want in market_types:
            if mt_c == _canonical(want):
                return True
        return False

    # Build candidates per market, then select ONE runner per market (strategy: widest spread)
    market_candidates: dict[tuple[int, int], list[tuple]] = {}
    for event in events:
        for market in event.get("markets", []):
            if not _market_matches(market.get("market-type", "")):
                continue
            if market.get("status") != "open":
                continue
            if not _passes_liquidity_filter(event, market):
                continue
            market_id = market.get("id", 0)
            if db.is_market_blacklisted(market_id):
                continue  # Blacklist: had Lay exit, never re-enter
            if db.is_market_closed_today(market_id):
                continue  # One-and-Done: already completed full cycle on this market today
            key = (event.get("id", 0), market_id)
            for runner in market.get("runners", []):
                if runner.get("status") != "open":
                    continue
                prices = runner.get("prices", [])
                best_back, best_lay = _get_best_back_lay(prices)
                if best_back is None or best_lay is None:
                    continue
                if best_lay is None or best_lay < config.MIN_ODDS or best_lay > config.MAX_ODDS:
                    continue  # Odds filter: sweet spot 1.50–4.00 (use best_lay for Lay entry)
                spread = (best_lay or 0) - (best_back or 0)
                if key not in market_candidates:
                    market_candidates[key] = []
                market_candidates[key].append((event, market, runner, best_back, best_lay, spread))

    # One runner per market: pick widest spread. Tiebreaker: min best_lay (liquidity for Lay).
    candidates = []
    for key, runners in market_candidates.items():
        if not runners:
            continue
        best = max(runners, key=lambda r: (r[5], -r[4]))  # max spread, then min best_lay
        candidates.append((best[0], best[1], best[2], best[3], best[4]))

    account = api.get_account()
    free_funds = float(account.get("free-funds", 0) or 0)
    # Phase 1: use small stake, max ~10% of bankroll per order
    max_stake = min(free_funds * 0.1, 5.0)
    if max_stake < 2.0:
        logger.info("Insufficient funds for Phase 1 (need >= £2)")
        db.insert_api_log("response", "BOT", "Phase 1", None, request_body="Skipped: insufficient funds (need free_funds >= £20)")
        return False

    if not candidates:
        # Diagnose why: count events, markets, runners, and runners with prices
        n_events = len(events)
        n_markets = sum(len(e.get("markets", [])) for e in events)
        n_runners = 0
        n_with_prices = 0
        market_types_seen = set()
        for e in events:
            for m in e.get("markets", []):
                market_types_seen.add(m.get("market-type", "?"))
                for r in m.get("runners", []):
                    n_runners += 1
                    prices = r.get("prices", [])
                    if prices:
                        n_with_prices += 1
        diag = (
            f"No candidates: {n_events} events, {n_markets} markets, {n_runners} runners, "
            f"{n_with_prices} with prices. Market types in data: {market_types_seen}. "
            f"Looking for: {market_types}"
        )
        logger.info("No tradeable events found: %s", diag)

        # When 0 events: try fallback without sport/after filters to diagnose
        fallback_msg = ""
        if n_events == 0:
            try:
                fallback = await api.get_events(
                    sport_ids=None,
                    include_prices=True,
                    price_depth=1,
                    states="open,suspended",
                    per_page=20,
                    pre_match_only=False,
                )
                sport_ids_in_fallback = list({e.get("sport-id") for e in fallback if e.get("sport-id") is not None})
                fallback_msg = (
                    f" Fallback (no sport/after filter): {len(fallback)} events. "
                    f"Sport IDs in fallback: {sport_ids_in_fallback}. "
                    f"→ Use API Debug 'Fetch sports' to get correct sport-ids."
                )
            except Exception as e:
                fallback_msg = f" Fallback failed: {e}"
            logger.info("Diagnostic: %s", fallback_msg)
            db.insert_api_log(
                "response", "BOT", "Phase 1 diagnostic", None,
                request_body=f"No candidates. Events={n_events}, markets={n_markets}, runners={n_runners}, with_prices={n_with_prices}. Market types in data: {market_types_seen}. Need: {market_types}.{fallback_msg}",
            )
        else:
            db.insert_api_log(
                "response", "BOT", "Phase 1", None,
                request_body=f"No candidates. Events={n_events}, markets={n_markets}, runners={n_runners}, with_prices={n_with_prices}. Market types in data: {market_types_seen}. Need: {market_types}",
            )
        return False

    # Rule 1 (Unmatched State Check): before ANY Phase 1 entry, check for existing orders.
    # Must include BOTH matched AND unmatched/pending. Skip if selection already has an order.
    exposed_runners: set[tuple[int, int]] = set()
    if db.get_paper_trading():
        exposed_runners = db.get_paper_exposed_runners()
    else:
        try:
            offers = await api.get_offers(statuses=["open", "matched"])
            for o in offers:
                mid, rid = o.get("market-id"), o.get("runner-id")
                if mid is not None and rid is not None:
                    exposed_runners.add((int(mid), int(rid)))
        except Exception as e:
            logger.warning("Could not fetch offers for pre-entry check: %s", e)

    db.insert_api_log(
        "response", "BOT", "Phase 1", None,
        request_body=f"Found {len(candidates)} candidates. Placing up to 5 Lay orders (stake £{round(min(free_funds * 0.1, 5.0), 2)})",
    )

    order_placed = False
    for event, market, runner, best_back, best_lay in candidates[:5]:
        market_id = market.get("id", 0)
        runner_id = runner["id"]
        if not _can_enter_selection(market_id, runner_id, exposed_runners):
            continue

        # Maker: Lay at exactly 2 ticks below best Lay (Lay-First strategy)
        ticks_below = max(1, config.LAY_TICKS_BELOW)
        lay_odds = _round_odds(best_lay - config.TICK_SIZE * ticks_below)
        if lay_odds < 1.02:
            continue  # Avoid invalid odds
        base_stake = round(min(max_stake, free_funds * 0.1), 2)
        # Lay liability cap: stake * (lay_odds - 1) <= free_funds
        max_by_liability = free_funds / (lay_odds - 1) if lay_odds > 1.0 else base_stake
        stake = round(min(base_stake, max_by_liability), 2)
        if stake < 2.0:
            continue

        try:
            if db.get_paper_trading():
                db.insert_paper_order(
                    market_id=market_id,
                    runner_id=runner_id,
                    event_id=event.get("id"),
                    event_name=event.get("name", ""),
                    market_name=market.get("name", ""),
                    runner_name=runner.get("name", ""),
                    side="lay",
                    odds=lay_odds,
                    stake=stake,
                    phase=1,
                )
                db.insert_paper_trade(
                    event_name=event.get("name", ""),
                    market_name=market.get("name", ""),
                    runner_name=runner.get("name", ""),
                    side="lay",
                    odds=lay_odds,
                    stake=stake,
                    phase=1,
                    reason="Phase 1: Maker Lay (2 ticks below best)",
                )
                exposed_runners.add((market_id, runner_id))  # Rule 1: block duplicate this loop
                db.insert_api_log(
                    "request", "PAPER", "Phase 1 Lay", None,
                    request_body=f"Would place: {runner.get('name')} Lay @ {lay_odds} x £{stake}",
                )
                logger.info(
                    "PAPER: Phase 1 Lay would place: %s @ %.2f x %.2f",
                    runner.get("name"),
                    lay_odds,
                    stake,
                )
            else:
                db.insert_api_log(
                    "request", "LIVE", "Phase 1 submit_offers", None,
                    request_body=f"Placing Lay: {runner.get('name')} @ {lay_odds} x £{stake}",
                )
                offers = [
                    {
                        "runner-id": runner["id"],
                        "side": "lay",
                        "odds": lay_odds,
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
                        side="lay",
                        odds=lay_odds,
                        stake=stake,
                        status=offer.get("status", "open"),
                        offer_id=offer.get("id"),
                        phase=1,
                        event_name=event.get("name", ""),
                        reason="Phase 1: Maker Lay (2 ticks below best)",
                    )
                    db.insert_position(
                        market_id=market.get("id"),
                        runner_id=runner["id"],
                        market_name=market.get("name", ""),
                        runner_name=runner.get("name", ""),
                        side="lay",
                        entry_odds=lay_odds,
                        entry_stake=stake,
                        offer_id=offer.get("id"),
                    )
                    exposed_runners.add((market_id, runner_id))
                    db.insert_api_log(
                        "response", "LIVE", "Phase 1 submit_offers", 200,
                        response_body=f"Order placed: status={offer.get('status')} offer_id={offer.get('id')}",
                    )
                    logger.info(
                        "Phase 1 Lay placed: %s @ %.2f x %.2f",
                        runner.get("name"),
                        lay_odds,
                        stake,
                    )
                    order_placed = True
                else:
                    db.insert_api_log("response", "LIVE", "Phase 1 submit_offers", None, error="submit_offers returned empty")
        except MarketSuspendedError:
            logger.warning("Market suspended, skipping")
            db.insert_api_log("response", "LIVE", "Phase 1", None, error="Market suspended")
        except Exception as e:
            logger.exception("Phase 1 order failed: %s", e)
            db.insert_api_log("response", "LIVE", "Phase 1", None, error=str(e))

        await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)

    # Poll for matched offers and hedge (Green Up)
    await hedge_all_matched_positions(api)
    return order_placed


def _runners_with_open_offers(offers: list[dict], side: str) -> set[tuple[int, int]]:
    """Return set of (market_id, runner_id) that already have an open offer of given side."""
    return {
        (int(o.get("market-id", 0) or 0), int(o.get("runner-id", 0) or 0))
        for o in offers
        if o.get("status") == "open" and o.get("side", "").lower() == side.lower()
    }


async def _process_hedge_confirmations(offers: list[dict]) -> None:
    """
    Rule 2 (Source of Truth): Only log trade complete when API returns MATCHED or SETTLED.
    Poll the offers we got - for each pending hedge, if it appears with matched/settled, process it.
    """
    if db.get_paper_trading():
        return
    offer_by_id = {o.get("id"): o for o in offers if o.get("id") is not None}
    for pend in db.get_pending_hedge_confirmations():
        oid = pend["hedge_offer_id"]
        o = offer_by_id.get(oid)
        if not o or o.get("status", "").lower() not in ("matched", "settled"):
            continue
        market_id = pend["market_id"]
        runner_id = pend["runner_id"]
        side = pend["side"]
        stake = pend["stake"]
        odds = pend["odds"]
        hedge_stake = float(o.get("stake", 0) or o.get("remaining", 0) or stake)
        pos = db.get_position_by_id(pend["position_id"]) if pend["position_id"] else None
        parent_offer = offer_by_id.get(pend.get("back_offer_id") or 0) if pend.get("back_offer_id") else None
        parent_stake = float(
            parent_offer.get("stake", 0) or parent_offer.get("remaining", 0)
            if parent_offer else 0
        )
        if pos:
            parent_stake = parent_stake or float(pos.get("entry_stake") or 0)
        lay_stake = hedge_stake if side == "lay" else parent_stake
        back_stake = parent_stake if side == "lay" else hedge_stake
        profit = _net_green_up_profit(lay_stake, back_stake)
        db.insert_trade(
            market_id=market_id,
            runner_id=runner_id,
            market_name=pend.get("market_name", ""),
            runner_name=pend.get("runner_name", ""),
            side=side,
            odds=odds,
            stake=stake,
            status=o.get("status", "matched"),
            offer_id=oid,
            phase=1,
            profit_loss=profit,
            event_name=pend.get("event_name", ""),
            reason="Hedge: Lay exit" if side == "lay" else "Hedge: Back exit",
        )
        if pos:
            db.update_position(pos["id"], "closed", profit)
        db.record_hedge_cooldown(market_id, runner_id)
        db.insert_closed_market(market_id, pend.get("event_id") or 0)
        db.insert_blacklisted_market(market_id, pend.get("event_id") or 0)
        db.delete_pending_hedge_confirmation(pend["id"])
        logger.info(
            "Hedge confirmed (API matched/settled): %s %s @ %.2f, profit £%.2f",
            pend.get("runner_name", ""),
            side,
            odds,
            profit,
        )


async def _process_phase2_leg_monitoring(api: MatchbookAPI) -> None:
    """
    Phase 2 Leg Timer and Bailout: protect against adverse selection (legging risk).
    If ONE leg of a Phase 2 Back+Lay pair is matched, start a timer. If the second leg
    is NOT matched within LEG_TIMEOUT_SEC, cancel the unmatched leg and fire a Market
    (Taker) order at best price to close the naked position.
    """
    if db.get_paper_trading():
        return
    pairs = db.get_active_phase2_leg_pairs()
    if not pairs:
        return
    offer_ids = []
    for p in pairs:
        bid, lid = p.get("back_offer_id"), p.get("lay_offer_id")
        if bid is not None:
            offer_ids.append(int(bid))
        if lid is not None:
            offer_ids.append(int(lid))
    offer_ids = list(set(offer_ids))
    try:
        offers = await api.get_offers(offer_ids=offer_ids, statuses=["open", "matched", "settled"])
    except Exception as e:
        logger.warning("Phase 2 leg monitoring: could not fetch offers: %s", e)
        return
    offer_by_id = {o.get("id"): o for o in offers if o.get("id") is not None}
    now = datetime.now(timezone.utc)
    timeout_sec = config.PHASE2_LEG_TIMEOUT_SEC

    for pair in pairs:
        back_o = offer_by_id.get(pair["back_offer_id"])
        lay_o = offer_by_id.get(pair["lay_offer_id"])
        back_matched = back_o and (back_o.get("status", "").lower() in ("matched", "settled"))
        lay_matched = lay_o and (lay_o.get("status", "").lower() in ("matched", "settled"))

        if back_matched and lay_matched:
            db.insert_hedge_initiated(pair["back_offer_id"])
            db.insert_hedge_initiated(pair["lay_offer_id"])
            db.mark_phase2_leg_pair_complete(pair["id"], "both_matched")
            logger.info("Phase 2 both legs matched: %s (market %s)", pair.get("runner_name", ""), pair.get("market_id"))
            continue

        if back_matched and not lay_matched:
            if not pair.get("leg_timer_started_at"):
                db.update_phase2_leg_timer(pair["id"], "back")
                logger.info("Phase 2 leg timer started: Back matched for %s, waiting %ds for Lay", pair.get("runner_name", ""), timeout_sec)
            else:
                try:
                    started = datetime.fromisoformat(str(pair["leg_timer_started_at"]).replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    started = now
                elapsed = (now - started).total_seconds()
                if elapsed >= timeout_sec:
                    logger.warning("Phase 2 leg bailout: Back matched, Lay unmatched after %.0fs. Cancelling Lay, placing taker hedge.", elapsed)
                    try:
                        await api.cancel_offers(offer_ids=[pair["lay_offer_id"]])
                    except Exception as e:
                        logger.warning("Failed to cancel Phase 2 Lay: %s", e)
                    db.insert_hedge_initiated(pair["back_offer_id"])
                    stake = float(back_o.get("stake", 0) or back_o.get("remaining", 0) or pair["stake"])
                    odds = float(back_o.get("odds", 0) or back_o.get("decimal-odds", 0) or pair["back_odds"])
                    ok, _ = await _hedge_with_retry(
                        api, pair["runner_id"], stake, odds,
                        pair.get("market_name", ""), pair.get("runner_name", ""),
                        market_id=pair["market_id"], event_id=pair.get("event_id", 0),
                        event_name=pair.get("event_name", ""),
                        back_offer_id=pair["back_offer_id"],
                        emergency_close=True,
                    )
                    db.mark_phase2_leg_pair_complete(pair["id"], "bailout_back_matched")
                    if ok:
                        logger.info("Phase 2 bailout: taker Lay hedge placed for %s", pair.get("runner_name", ""))
                    await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)
            continue

        if lay_matched and not back_matched:
            if not pair.get("leg_timer_started_at"):
                db.update_phase2_leg_timer(pair["id"], "lay")
                logger.info("Phase 2 leg timer started: Lay matched for %s, waiting %ds for Back", pair.get("runner_name", ""), timeout_sec)
            else:
                try:
                    started = datetime.fromisoformat(str(pair["leg_timer_started_at"]).replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    started = now
                elapsed = (now - started).total_seconds()
                if elapsed >= timeout_sec:
                    logger.warning("Phase 2 leg bailout: Lay matched, Back unmatched after %.0fs. Cancelling Back, placing taker hedge.", elapsed)
                    try:
                        await api.cancel_offers(offer_ids=[pair["back_offer_id"]])
                    except Exception as e:
                        logger.warning("Failed to cancel Phase 2 Back: %s", e)
                    db.insert_hedge_initiated(pair["lay_offer_id"])
                    stake = float(lay_o.get("stake", 0) or lay_o.get("remaining", 0) or pair["stake"])
                    odds = float(lay_o.get("odds", 0) or lay_o.get("decimal-odds", 0) or pair["lay_odds"])
                    ok, _ = await _hedge_lay_with_retry(
                        api, pair["runner_id"], stake, odds,
                        pair.get("market_name", ""), pair.get("runner_name", ""),
                        market_id=pair["market_id"], event_id=pair.get("event_id", 0),
                        event_name=pair.get("event_name", ""),
                        lay_offer_id=pair["lay_offer_id"],
                        emergency_close=True,
                    )
                    db.mark_phase2_leg_pair_complete(pair["id"], "bailout_lay_matched")
                    if ok:
                        logger.info("Phase 2 bailout: taker Back hedge placed for %s", pair.get("runner_name", ""))
                    await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)
            continue


async def hedge_all_matched_positions(
    api: MatchbookAPI, hedge_all: bool = False
) -> bool:
    """
    Fetch matched offers and hedge each (Back with Lay, Lay with Back).
    Exit state check: never place a hedge if an open hedge order already exists.
    hedge_all=True: process all matched (panic hedge). False: one per call (bot cycle).
    Rule 2: Process pending hedge confirmations (API source of truth) before placing new hedges.
    Phase 2 offers are excluded: handled by _process_phase2_leg_monitoring.
    """
    hedge_placed = False
    if not db.get_paper_trading():
        offers = await api.get_offers(statuses=["open", "matched"])
        await _process_hedge_confirmations(offers)
        await _process_phase2_leg_monitoring(api)
    else:
        offers = []

    # Hard Lock: use ONLY local state. Never use exchange Matched/Unmatched/Partially Matched.
    pending = db.get_pending_hedge_confirmations()
    child_offer_ids = {p["hedge_offer_id"] for p in pending}
    parent_ids_already_hedged = db.get_hedge_initiated_parent_ids()
    parent_ids_already_hedged.update(p["back_offer_id"] for p in pending if p.get("back_offer_id"))
    parent_ids_already_hedged.update(db.get_phase2_offer_ids())  # Exclude Phase 2 legs

    open_lay_runners = _runners_with_open_offers(offers, "lay")
    open_back_runners = _runners_with_open_offers(offers, "back")
    count = 0
    for offer in offers:
        # Ignore partial fills: never use exchange status to decide. Only process fully "matched".
        if offer.get("status") != "matched":
            continue
        parent_offer_id = offer.get("id")
        if parent_offer_id is None:
            continue
        # Execution Block: use ONLY local state. If Parent already hedge_pending or initiated, PASS.
        if parent_offer_id in parent_ids_already_hedged:
            continue  # Hard Lock: already placed hedge for this Parent
        if parent_offer_id in child_offer_ids:
            continue  # Ignore: this is our Child (never hedge the hedge)
        pos = db.get_position_by_offer_id(parent_offer_id)
        if pos and pos.get("status") == "hedge_pending":
            continue  # Hard Lock: Parent marked hedge_pending locally
        market_id = int(offer.get("market-id") or 0)
        if db.is_market_blacklisted(market_id):
            continue  # Ignore: market marked CLOSED/HEDGED
        runner_id = offer.get("runner-id")
        stake = float(offer.get("stake", 0) or offer.get("remaining", 0))
        odds = float(offer.get("odds", 0) or offer.get("decimal-odds", 0))
        if stake <= 0 or odds <= 0 or not runner_id:
            continue
        key = (int(market_id or 0), int(runner_id or 0))
        market_name = offer.get("market-name", "")
        runner_name = offer.get("runner-name", "")
        event_id = offer.get("event-id") or 0
        if offer.get("side") == "back":
            if key in open_lay_runners:
                continue  # Exit state check: Lay hedge already pending, don't stack
            if not db.get_paper_trading():
                # Hard Lock: mark Parent hedge_pending BEFORE any API call
                db.insert_hedge_initiated(parent_offer_id)
                if pos:
                    db.update_position_to_hedge_pending(pos["id"])
                db.insert_blacklisted_market(market_id, event_id)
                ok, _ = await _hedge_with_retry(
                    api, runner_id, stake, odds,
                    market_name, runner_name,
                    market_id=market_id, event_id=event_id,
                    event_name=offer.get("event-name", ""),
                    back_offer_id=offer.get("id"),
                )
                if ok:
                    hedge_placed = True
        elif offer.get("side") == "lay":
            if key in open_back_runners:
                continue  # Exit state check: Back hedge already pending, don't stack
            if not db.get_paper_trading():
                # Hard Lock: mark Parent hedge_pending BEFORE any API call
                db.insert_hedge_initiated(parent_offer_id)
                if pos:
                    db.update_position_to_hedge_pending(pos["id"])
                db.insert_blacklisted_market(market_id, event_id)
                # Lay-first stop-loss: if price dropped N ticks below our Lay odds, emergency exit
                emergency = False
                current_lay = await _fetch_lay_odds(api, runner_id)
                if current_lay is not None and odds > 0:
                    threshold = odds - config.TICK_SIZE * config.LAY_STOP_LOSS_TICKS
                    if current_lay < threshold:
                        emergency = True
                        logger.info(
                            "Lay stop-loss: %s current Lay %.2f < %.2f (matched %.2f - %d ticks), emergency hedge",
                            runner_name, current_lay, threshold, odds, config.LAY_STOP_LOSS_TICKS,
                        )
                ok, _ = await _hedge_lay_with_retry(
                    api, runner_id, stake, odds,
                    market_name, runner_name,
                    market_id=market_id, event_id=event_id,
                    event_name=offer.get("event-name", ""),
                    lay_offer_id=offer.get("id"),
                    emergency_close=emergency,
                )
                if ok:
                    hedge_placed = True
        count += 1
        await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)
        if not hedge_all:
            break  # One per bot cycle
    if hedge_all and count > 0:
        logger.info("Panic hedge: closed %d matched position(s)", count)
    return hedge_placed


async def _process_paper_simulated_fills(api: MatchbookAPI) -> None:
    """
    Rule 3 (Simulated Fill): Paper mode only. For each open paper order, check market price.
    If price crosses our odds, mark MATCHED and log theoretical profit. Do not poll order status.
    """
    if not db.get_paper_trading():
        return
    orders = db.get_open_paper_orders()
    if not orders:
        return
    market_ids = list({o["market_id"] for o in orders})
    events = await api.get_events(
        sport_ids=_get_sport_ids(),
        include_prices=True,
        price_depth=1,
        states="open",
        per_page=100,
    )
    # Build runner_id -> (best_back, best_lay) for each market
    runner_prices: dict[tuple[int, int], tuple[Optional[float], Optional[float]]] = {}
    for ev in events:
        for mkt in ev.get("markets", []):
            if mkt.get("id") not in market_ids:
                continue
            mid = mkt["id"]
            for runner in mkt.get("runners", []):
                rid = runner.get("id")
                if rid is not None:
                    best_back, best_lay = _get_best_back_lay(runner.get("prices", []))
                    runner_prices[(mid, rid)] = (best_back, best_lay)
    for order in orders:
        key = (order["market_id"], order["runner_id"])
        prices = runner_prices.get(key)
        if not prices or (prices[0] is None and prices[1] is None):
            continue
        best_back, best_lay = prices
        odds = order["odds"]
        stake = order["stake"]
        side = (order["side"] or "").lower()
        filled = False
        profit = 0.0
        if side == "back":
            if best_lay is not None and best_lay <= odds:
                filled = True
                if best_lay > 0:
                    profit = _locked_in_profit_back_hedge(stake, odds, best_lay)
        elif side == "lay":
            if best_back is not None and best_back >= odds:
                filled = True
                if best_back > 0:
                    profit = _locked_in_profit_lay_hedge(stake, odds, best_back)
        if filled:
            db.update_paper_order_matched(order["id"])
            db.insert_paper_trade_with_profit(
                event_name=order.get("event_name", ""),
                market_name=order.get("market_name", ""),
                runner_name=order.get("runner_name", ""),
                side=side,
                odds=odds,
                stake=stake,
                phase=order.get("phase", 1),
                reason="Simulated fill: price crossed",
                profit_loss=profit,
            )
            logger.info(
                "PAPER: Simulated fill %s @ %.2f for %s, theoretical profit £%.2f",
                side,
                odds,
                order.get("runner_name", ""),
                profit,
            )


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


async def _run_phase2(api: MatchbookAPI) -> bool:
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

    db.insert_api_log(
        "response", "BOT", "get_events", None,
        request_body=f"Phase 2: got {len(events)} events (sport_ids={sport_ids}, market_types={market_types})",
    )

    def _norm(s: str) -> str:
        return (s or "").lower().replace("-", "_").replace(".", "_").replace(" ", "_")

    def _canonical(s: str) -> str:
        n = _norm(s)
        if n in ("over_under_2_5", "over_under_25"):
            return "over_under_25"
        return n

    def _market_matches(mt: str) -> bool:
        if not mt:
            return False
        mt_c = _canonical(mt)
        for want in market_types:
            if mt_c == _canonical(want):
                return True
        return False

    # Build candidates per market, then select ONE runner per market (widest spread)
    market_candidates: dict[tuple[int, int], list[tuple]] = {}
    for event in events:
        for market in event.get("markets", []):
            if not _market_matches(market.get("market-type", "")):
                continue
            if market.get("status") != "open":
                continue
            if not _passes_liquidity_filter(event, market):
                continue
            market_id = market.get("id", 0)
            if db.is_market_blacklisted(market_id):
                continue  # Blacklist: had Lay exit, never re-enter
            if db.is_market_closed_today(market_id):
                continue  # One-and-Done: already completed full cycle on this market today
            key = (event.get("id", 0), market_id)
            for runner in market.get("runners", []):
                if runner.get("status") != "open":
                    continue
                prices = runner.get("prices", [])
                best_back, best_lay = _get_best_back_lay(prices)
                if best_back is None or best_lay is None:
                    continue
                if best_back < config.MIN_ODDS or best_back > config.MAX_ODDS:
                    continue  # Odds filter: sweet spot 1.50–4.00 only
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
                liability = _lay_liability(stake, lay_odds)
                if liability > free_funds:
                    continue
                spread = lay_odds - back_odds
                if key not in market_candidates:
                    market_candidates[key] = []
                market_candidates[key].append((event, market, runner, back_odds, lay_odds, stake, spread))

    # One runner per market: pick widest harvestable spread
    phase2_candidates = []
    for key, runners in market_candidates.items():
        if not runners:
            continue
        best = max(runners, key=lambda r: (r[6], -r[3]))  # max spread, then min back_odds
        phase2_candidates.append(best)

    # Rule 1: Check for existing orders (matched + unmatched) before Phase 2 entry
    exposed_runners: set[tuple[int, int]] = set()
    if db.get_paper_trading():
        exposed_runners = db.get_paper_exposed_runners()
    else:
        try:
            offers = await api.get_offers(statuses=["open", "matched"])
            for o in offers:
                mid, rid = o.get("market-id"), o.get("runner-id")
                if mid is not None and rid is not None:
                    exposed_runners.add((int(mid), int(rid)))
        except Exception as e:
            logger.warning("Could not fetch offers for Phase 2 pre-entry check: %s", e)

    for event, market, runner, back_odds, lay_odds, stake, _ in phase2_candidates:
        market_id = market.get("id", 0)
        runner_id = runner["id"]
        if not _can_enter_selection(market_id, runner_id, exposed_runners):
            continue
        try:
            if db.get_paper_trading():
                db.insert_paper_order(
                    market_id=market_id,
                    runner_id=runner_id,
                    event_id=event.get("id"),
                    event_name=event.get("name", ""),
                    market_name=market.get("name", ""),
                    runner_name=runner.get("name", ""),
                    side="back",
                    odds=back_odds,
                    stake=stake,
                    phase=2,
                )
                db.insert_paper_order(
                    market_id=market_id,
                    runner_id=runner_id,
                    event_id=event.get("id"),
                    event_name=event.get("name", ""),
                    market_name=market.get("name", ""),
                    runner_name=runner.get("name", ""),
                    side="lay",
                    odds=lay_odds,
                    stake=stake,
                    phase=2,
                )
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
                db.insert_api_log(
                    "request", "PAPER", "Phase 2 Back+Lay", None,
                    request_body=f"Would place: {runner.get('name')} Back @ {back_odds} Lay @ {lay_odds} x £{stake}",
                )
                logger.info(
                    "PAPER: Phase 2 would place Back %.2f Lay %.2f @ %.2f for %s",
                    back_odds, lay_odds, stake, runner.get("name"),
                )
            else:
                db.insert_api_log(
                    "request", "LIVE", "Phase 2 submit_offers", None,
                    request_body=f"Placing Back+Lay: {runner.get('name')} Back @ {back_odds} Lay @ {lay_odds} x £{stake}",
                )
                offers = [
                    {"runner-id": runner["id"], "side": "back", "odds": back_odds, "stake": stake, "keep-in-play": False},
                    {"runner-id": runner["id"], "side": "lay", "odds": lay_odds, "stake": stake, "keep-in-play": False},
                ]
                result = await api.submit_offers(offers)
                if result:
                    db.insert_blacklisted_market(market_id, event.get("id", 0))  # Rule 3: instant blacklist on Phase 2 Lay
                    back_offer = result[0] if len(result) > 0 else {}
                    lay_offer = result[1] if len(result) > 1 else {}
                    back_offer_id = back_offer.get("id")
                    lay_offer_id = lay_offer.get("id")
                    if back_offer_id and lay_offer_id:
                        db.insert_phase2_leg_pair(
                            back_offer_id=back_offer_id,
                            lay_offer_id=lay_offer_id,
                            market_id=market_id,
                            runner_id=runner_id,
                            event_id=event.get("id", 0),
                            stake=stake,
                            back_odds=back_odds,
                            lay_odds=lay_odds,
                            market_name=market.get("name", ""),
                            runner_name=runner.get("name", ""),
                            event_name=event.get("name", ""),
                        )
                    db.insert_api_log("response", "LIVE", "Phase 2 submit_offers", 200, response_body=f"Orders placed: {len(result)} offers")
                    logger.info("Phase 2 orders placed: %s Back %.2f Lay %.2f @ %.2f", runner.get("name"), back_odds, lay_odds, stake)
                    await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)
                    return True  # One market per cycle, order placed
                else:
                    db.insert_api_log("response", "LIVE", "Phase 2 submit_offers", None, error="submit_offers returned empty")
        except MarketSuspendedError:
            logger.warning("Market suspended")
            db.insert_api_log("response", "LIVE", "Phase 2", None, error="Market suspended")
        except Exception as e:
            logger.exception("Phase 2 order failed: %s", e)
            db.insert_api_log("response", "LIVE", "Phase 2", None, error=str(e))
        await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)
        return False  # One market per cycle, no order placed


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

        # Startup State Recovery: adopt orphaned exchange orders before any new trades (runs even when paused)
        await _run_startup_state_recovery(api)

        if not trading_enabled:
            logger.info("Bot is paused. Snapshot recorded, no orders placed.")
            db.insert_api_log("response", "BOT", "main", None, request_body="Skipped: Trading disabled (toggle in sidebar)")
            return

        # Pre-match only: close orders for events starting soon
        order_placed = await _close_events_before_start(api)

        # Cancel open orders stuck in low-volume markets (<£1k)
        await _cancel_low_volume_orders(api)

        # Cancel dead trades: open orders with odds > 4.50 (massive underdogs)
        await _cancel_high_odds_orders(api)

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
                msg = (
                    f"Daily stop-loss triggered: {daily_loss_pct:.1f}% loss "
                    f"(limit {stop_loss_pct:.1f}%). Trading paused."
                )
                logger.warning("%s", msg)
                asyncio.to_thread(alerts.send_alert, msg, "stop_loss")
                return

        phase = 2 if free_funds >= config.PHASE2_MIN_BANKROLL else 1
        logger.info("Bankroll: £%.2f, Phase: %d", free_funds, phase)

        # Rule 3: Paper mode - simulate fills before new entries
        await _process_paper_simulated_fills(api)

        if phase == 1:
            order_placed = order_placed or await _run_phase1(api)
        else:
            order_placed = order_placed or await _run_phase2(api)

        # Hedge any matched positions (Back or Lay) from this or previous cycles
        hedge_placed = await hedge_all_matched_positions(api)
        order_placed = order_placed or hedge_placed

        # API latency buffer: wait 3s after Live order so Matchbook can update status
        if order_placed and not db.get_paper_trading():
            logger.info("Order placed: waiting 3s for Matchbook API to update status")
            await asyncio.sleep(3)

    finally:
        await api.close()


def main() -> None:
    """Entry point for the bot."""
    try:
        asyncio.run(_main_loop())
    except Exception as e:
        msg = f"Bot error: {e}\n\n{traceback.format_exc()}"
        alerts.send_alert(msg, "error")
        raise


if __name__ == "__main__":
    main()
