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
    Compute the Back stake required to Green Up a Lay position.
    Formula: Back Stake = (Lay Stake * Lay Odds) / Back Odds
    """
    if back_odds <= 0:
        return 0.0
    return round((lay_stake * lay_odds) / back_odds, 2)


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
                    db.insert_trade(
                        market_id=market_id,
                        runner_id=runner_id,
                        market_name=market_name,
                        runner_name=runner_name,
                        side="lay",
                        odds=lay_odds,
                        stake=lay_stake,
                        status=result[0].get("status", "open"),
                        offer_id=result[0].get("id"),
                        phase=1,
                        profit_loss=profit,
                    )
                    pos = db.get_position_by_offer_id(back_offer_id) if back_offer_id else None
                    if pos:
                        db.update_position(pos["id"], "closed", profit)
                    db.record_hedge_cooldown(market_id, runner_id)
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
                    db.insert_trade(
                        market_id=market_id,
                        runner_id=runner_id,
                        market_name=market_name,
                        runner_name=runner_name,
                        side="back",
                        odds=back_odds,
                        stake=back_stake,
                        status=result[0].get("status", "open"),
                        offer_id=result[0].get("id"),
                        phase=1,
                        profit_loss=profit,
                    )
                    pos = db.get_position_by_offer_id(lay_offer_id) if lay_offer_id else None
                    if pos:
                        db.update_position(pos["id"], "closed", profit)
                    db.record_hedge_cooldown(market_id, runner_id)
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

        # Hedge matched positions (Back with Lay, Lay with Back)
        for o in event_offers:
            if o.get("status") != "matched":
                continue
            runner_id = o.get("runner-id")
            stake = float(o.get("stake", 0) or o.get("remaining", 0))
            odds = float(o.get("odds", 0) or o.get("decimal-odds", 0))
            if stake <= 0 or odds <= 0 or not runner_id:
                continue
            market_id = o.get("market-id") or 0
            if o.get("side") == "back":
                if not db.get_paper_trading():
                    await _hedge_with_retry(
                        api, runner_id, stake, odds,
                        o.get("market-name", ""), o.get("runner-name", ""),
                        market_id=market_id, back_offer_id=o.get("id"),
                        emergency_close=True,  # Time Stop: cross spread for immediate exit
                    )
            elif o.get("side") == "lay":
                if not db.get_paper_trading():
                    await _hedge_lay_with_retry(
                        api, runner_id, stake, odds,
                        o.get("market-name", ""), o.get("runner-name", ""),
                        market_id=market_id, lay_offer_id=o.get("id"),
                        emergency_close=True,  # Time Stop: cross spread for immediate exit
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
            key = (event.get("id", 0), market.get("id", 0))
            for runner in market.get("runners", []):
                if runner.get("status") != "open":
                    continue
                prices = runner.get("prices", [])
                best_back, best_lay = _get_best_back_lay(prices)
                if best_back is None or best_lay is None:
                    continue
                if best_back < config.MIN_ODDS or best_back > config.MAX_ODDS:
                    continue  # Odds filter: sweet spot 1.50–4.00 only
                spread = (best_lay or 0) - (best_back or 0)
                if key not in market_candidates:
                    market_candidates[key] = []
                market_candidates[key].append((event, market, runner, best_back, best_lay, spread))

    # One runner per market: pick widest spread (best scalping edge). Tiebreaker: lower odds (liquidity).
    candidates = []
    for key, runners in market_candidates.items():
        if not runners:
            continue
        best = max(runners, key=lambda r: (r[5], -r[3]))  # max spread, then min best_back
        candidates.append((best[0], best[1], best[2], best[3], best[4]))

    account = api.get_account()
    free_funds = float(account.get("free-funds", 0) or 0)
    # Phase 1: use small stake, max ~10% of bankroll per order
    max_stake = min(free_funds * 0.1, 5.0)
    if max_stake < 2.0:
        logger.info("Insufficient funds for Phase 1 (need >= £2)")
        db.insert_api_log("response", "BOT", "Phase 1", None, request_body="Skipped: insufficient funds (need free_funds >= £20)")
        return

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
        return

    # Fetch current offers to avoid re-entering selections we already have exposure on
    exposed_runners: set[tuple[int, int]] = set()
    if not db.get_paper_trading():
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
        request_body=f"Found {len(candidates)} candidates. Placing up to 5 Back orders (stake £{round(min(free_funds * 0.1, 5.0), 2)})",
    )

    for event, market, runner, best_back, best_lay in candidates[:5]:
        market_id = market.get("id", 0)
        runner_id = runner["id"]
        if not _can_enter_selection(market_id, runner_id, exposed_runners):
            continue

        # Maker: Back at least 1-2 ticks above best Back (provide liquidity, wait for market)
        ticks_above = max(1, config.BACK_TICKS_ABOVE)
        back_odds = _round_odds(best_back + config.TICK_SIZE * ticks_above)
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
                    reason="Phase 1: Maker Back (2 ticks above best)",
                )
                db.insert_api_log(
                    "request", "PAPER", "Phase 1 Back", None,
                    request_body=f"Would place: {runner.get('name')} Back @ {back_odds} x £{stake}",
                )
                logger.info(
                    "PAPER: Phase 1 Back would place: %s @ %.2f x %.2f",
                    runner.get("name"),
                    back_odds,
                    stake,
                )
            else:
                db.insert_api_log(
                    "request", "LIVE", "Phase 1 submit_offers", None,
                    request_body=f"Placing Back: {runner.get('name')} @ {back_odds} x £{stake}",
                )
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
                    exposed_runners.add((market_id, runner_id))
                    db.insert_api_log(
                        "response", "LIVE", "Phase 1 submit_offers", 200,
                        response_body=f"Order placed: status={offer.get('status')} offer_id={offer.get('id')}",
                    )
                    logger.info(
                        "Phase 1 Back placed: %s @ %.2f x %.2f",
                        runner.get("name"),
                        back_odds,
                        stake,
                    )
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


async def hedge_all_matched_positions(
    api: MatchbookAPI, hedge_all: bool = False
) -> None:
    """
    Fetch matched offers and hedge each (Back with Lay, Lay with Back).
    hedge_all=True: process all matched (panic hedge). False: one per call (bot cycle).
    """
    offers = await api.get_offers(statuses=["open", "matched"])
    count = 0
    for offer in offers:
        if offer.get("status") != "matched":
            continue
        runner_id = offer.get("runner-id")
        stake = float(offer.get("stake", 0) or offer.get("remaining", 0))
        odds = float(offer.get("odds", 0) or offer.get("decimal-odds", 0))
        if stake <= 0 or odds <= 0 or not runner_id:
            continue
        market_id = offer.get("market-id") or 0
        market_name = offer.get("market-name", "")
        runner_name = offer.get("runner-name", "")
        if offer.get("side") == "back":
            if not db.get_paper_trading():
                await _hedge_with_retry(
                    api, runner_id, stake, odds,
                    market_name, runner_name,
                    market_id=market_id, back_offer_id=offer.get("id"),
                )
        elif offer.get("side") == "lay":
            if not db.get_paper_trading():
                await _hedge_lay_with_retry(
                    api, runner_id, stake, odds,
                    market_name, runner_name,
                    market_id=market_id, lay_offer_id=offer.get("id"),
                )
        count += 1
        await asyncio.sleep(config.RATE_LIMIT_DELAY_MS / 1000.0)
        if not hedge_all:
            break  # One per bot cycle
    if hedge_all and count > 0:
        logger.info("Panic hedge: closed %d matched position(s)", count)


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
            key = (event.get("id", 0), market.get("id", 0))
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

    # Fetch current offers for pre-entry check
    exposed_runners: set[tuple[int, int]] = set()
    if not db.get_paper_trading():
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
                    db.insert_api_log("response", "LIVE", "Phase 2 submit_offers", 200, response_body=f"Orders placed: {len(result)} offers")
                    logger.info("Phase 2 orders placed: %s Back %.2f Lay %.2f @ %.2f", runner.get("name"), back_odds, lay_odds, stake)
                else:
                    db.insert_api_log("response", "LIVE", "Phase 2 submit_offers", None, error="submit_offers returned empty")
        except MarketSuspendedError:
            logger.warning("Market suspended")
            db.insert_api_log("response", "LIVE", "Phase 2", None, error="Market suspended")
        except Exception as e:
            logger.exception("Phase 2 order failed: %s", e)
            db.insert_api_log("response", "LIVE", "Phase 2", None, error=str(e))
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
            db.insert_api_log("response", "BOT", "main", None, request_body="Skipped: Trading disabled (toggle in sidebar)")
            return

        # Pre-match only: close orders for events starting soon
        await _close_events_before_start(api)

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

        if phase == 1:
            await _run_phase1(api)
        else:
            await _run_phase2(api)

        # Hedge any matched positions (Back or Lay) from this or previous cycles
        await hedge_all_matched_positions(api)

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
