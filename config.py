"""
Configuration for the Matchbook Automated Trading System.
Configurable sport-ids, tick sizes, phase thresholds, and market types.
Supports both Football and Political markets via SPORT_IDS / MARKET_TYPES.
"""

import os
from typing import Optional

# Phase thresholds (GBP)
PHASE1_MAX_BANKROLL = 200
PHASE2_MIN_BANKROLL = 200
STARTING_BANKROLL = 25
TARGET_BANKROLL = 5000
DAILY_ROI_TARGET_PCT = 5.0

# Matchbook commission: 2% on net winnings (UK/ROI/Channel Islands). Other regions: 4%.
# Commission applies only to profits; balance from API is already post-commission.
COMMISSION_RATE = 0.02


def gross_roi_target_pct(
    net_target_pct: Optional[float] = None,
    commission_rate: Optional[float] = None,
) -> float:
    """
    Gross ROI % needed to achieve net_target_pct after commission.
    net_target_pct defaults to DAILY_ROI_TARGET_PCT.
    commission_rate defaults to COMMISSION_RATE (use db.get_commission_rate() for user override).
    """
    net = net_target_pct if net_target_pct is not None else DAILY_ROI_TARGET_PCT
    rate = commission_rate if commission_rate is not None else COMMISSION_RATE
    if rate >= 1.0:
        return net
    return net / (1.0 - rate)


def net_profit_after_commission(
    gross_profit: float,
    commission_rate: Optional[float] = None,
) -> float:
    """Apply commission to gross profit. Returns net profit (what you keep)."""
    if gross_profit <= 0:
        return gross_profit
    rate = commission_rate if commission_rate is not None else COMMISSION_RATE
    return gross_profit * (1.0 - rate)


# Daily stop-loss: pause trading if daily loss exceeds this % of start-of-day bankroll
DAILY_STOP_LOSS_PCT = 10.0

# Market focus: configurable for Football, Political, or both
# Sport IDs: 1 = American Football (NOT Soccer). Use API Debug → Fetch sports for full list.
SPORT_IDS = [15]  # Fallback; override in dashboard from Fetch sports
# Match Odds (one_x_two / money_line), O/U 2.5 Goals
MARKET_TYPES = ["one_x_two", "money_line", "over_under_25"]

# Tick size for decimal odds (e.g. 2.0 -> 2.02 -> 2.04)
TICK_SIZE = 0.02
# Entry: Lay-first strategy - Lay at 2 ticks below best Lay
LAY_TICKS_BELOW = 2
# Lay-first stop-loss: hedge (Back) if market drops this many ticks below matched Lay odds (4–6 ticks gives room to breathe)
LAY_STOP_LOSS_TICKS = 5
# Exit hedge: Lay placed at best_lay - N ticks (maker). Time Stop overrides.
HEDGE_LAY_TICKS_BELOW = 1
# Exit hedge: Back placed at best_back - N ticks (maker). Time Stop overrides.
HEDGE_BACK_TICKS_BELOW = 1
# Phase 2: spread harvesting - Back at best+1 tick, Lay at best-1 tick
PHASE2_BACK_TICKS_ABOVE = 1
PHASE2_LAY_TICKS_BELOW = 1
# Phase 2 Leg Timer: seconds to wait for second leg before bailout (adverse selection protection)
PHASE2_LEG_TIMEOUT_SEC = 10

# Entry cooldown: seconds after a trade is hedged before re-entering same selection
ENTRY_COOLDOWN_SEC = 60

# Rate limiting (ms between API requests)
RATE_LIMIT_DELAY_MS = 100
# Market suspended: retry hedge every N seconds
HEDGE_RETRY_INTERVAL_SEC = 2
MAX_HEDGE_RETRIES = 30

# Database path (override via DB_PATH env for Docker)
DB_PATH = os.getenv("DB_PATH", "trading.db")

# Odds filter (The Sweet Spot): only trade selections between 1.50 and 4.00
MIN_ODDS = 1.50   # Skip heavy favorites
MAX_ODDS = 4.00   # Skip massive underdogs (wide spreads, poor scalping)
MAX_ODDS_CANCEL = 4.50  # Cancel open orders above this (dead trades)

# Liquidity filter: only trade in high-volume markets
MIN_EVENT_VOLUME = 10_000   # £10k min event volume (excludes obscure leagues)
MIN_MARKET_VOLUME = 2_000  # £2k min market volume
LOW_VOLUME_CANCEL_THRESHOLD = 1_000  # Cancel open orders in markets below this
# Optional: category IDs for top-tier (Premier League, etc.). Empty = volume only.
ALLOWED_CATEGORY_IDS: list[int] = []

# Pre-match only: exclude in-play/live events (volatile, fast-moving)
# When True, only fetch events that start in the future
PRE_MATCH_ONLY = True

# When pre-match only: close all orders/positions this many minutes before event start
CLOSE_BEFORE_START_MINUTES = 5

# API base URLs
API_BASE_BPAPI = "https://api.matchbook.com/bpapi/rest"
API_BASE_EDGE = "https://api.matchbook.com/edge/rest"
API_TIMEOUT_SEC = 30
