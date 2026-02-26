"""
Configuration for the Matchbook Automated Trading System.
Configurable sport-ids, tick sizes, phase thresholds, and market types.
Supports both Football and Political markets via SPORT_IDS / MARKET_TYPES.
"""

# Phase thresholds (GBP)
PHASE1_MAX_BANKROLL = 200
PHASE2_MIN_BANKROLL = 200
STARTING_BANKROLL = 25
TARGET_BANKROLL = 5000
DAILY_ROI_TARGET_PCT = 5.0

# Market focus: configurable for Football, Political, or both
# Football sport-id is typically 1; political uses category-ids
# See Matchbook API docs for sport-ids and category-ids
SPORT_IDS = [1]  # 1 = Football/Soccer; add political category ids as needed
# Match Odds (one_x_two / money_line), O/U 2.5 Goals
MARKET_TYPES = ["one_x_two", "money_line", "over_under_25"]

# Tick size for decimal odds (e.g. 2.0 -> 2.02 -> 2.04)
TICK_SIZE = 0.02
# Phase 1: place Back order this many ticks above best available
BACK_TICKS_ABOVE = 2
# Phase 2: spread harvesting - Back at best+1 tick, Lay at best-1 tick
PHASE2_BACK_TICKS_ABOVE = 1
PHASE2_LAY_TICKS_BELOW = 1

# Rate limiting (ms between API requests)
RATE_LIMIT_DELAY_MS = 100
# Market suspended: retry hedge every N seconds
HEDGE_RETRY_INTERVAL_SEC = 2
MAX_HEDGE_RETRIES = 30

# Database path
DB_PATH = "trading.db"

# API base URLs
API_BASE_BPAPI = "https://api.matchbook.com/bpapi/rest"
API_BASE_EDGE = "https://api.matchbook.com/edge/rest"
API_TIMEOUT_SEC = 30
