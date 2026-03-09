# Matchbook Automated Trading System

A local automated trading system for the Matchbook Exchange with a headless Python bot and Streamlit dashboard. 

## Setup

1. Create virtual environment and install dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

2. Copy `.env.example` to `.env` and add your Matchbook credentials:

```bash
cp .env.example .env
# Edit .env: MATCHBOOK_USER, MATCHBOOK_PASSWORD
```

## Usage

**Run the bot** (Phase 1 scalping / Phase 2 market making):

```bash
.venv/bin/python bot.py
```

**Run the dashboard**:

```bash
.venv/bin/streamlit run app.py
```

## Docker Deployment

```bash
# Ensure .env exists with MATCHBOOK_USER and MATCHBOOK_PASSWORD
docker compose up -d
```

- **Bot**: Runs continuously; default cycle every 5 seconds (configurable via `BOT_CYCLE_INTERVAL_SEC` in `.env`), shares SQLite DB with dashboard
- **Dashboard**: http://localhost:8502

## When the Bot Trades

- **Docker**: Each cycle (default every 5s) the bot logs in, records a bankroll snapshot, then places orders if trading is enabled.
- **Cycle tuning**: Set `BOT_CYCLE_INTERVAL_SEC` in `.env` (recommended 3-10s for fast hedge reaction).
- **On/Off toggle**: Use the sidebar "Trading enabled" toggle to pause. When paused, the bot still records snapshots but places no orders.
- **Daily stop-loss**: If today's loss exceeds `DAILY_STOP_LOSS_PCT` (default 10%) of start-of-day bankroll, trading pauses. Clear via sidebar to resume.
- **Paper trading**: Toggle in sidebar. No real orders—bot runs logic and logs "would place" only. Safe for testing.
- **Pre-match only**: Default on. Bot only trades events that haven't started (excludes in-play). Toggle in sidebar.
- **Alerts**: Optional Telegram, Discord, and/or email notifications for stop-loss, errors, hedge failures, and panic hedge. Configure via `ALERT_*` env vars (see `.env.example`).

## Configuration

Edit `config.py` to adjust:

- `SPORT_IDS` – Football (1) or political category IDs
- `MARKET_TYPES` – e.g. `one_x_two`, `over_under_25`
- `PHASE1_MAX_BANKROLL` / `PHASE2_MIN_BANKROLL` – Phase thresholds (£200)
- `TICK_SIZE`, `BACK_TICKS_ABOVE` – Order placement parameters

## Architecture

- **db.py** – SQLite schema (trades, positions, bankroll_snapshots)
- **matchbook_api.py** – Async API wrapper (auth, events, offers)
- **bot.py** – Trading logic (Green Up formula, liability check, suspend retry)
- **app.py** – Streamlit dashboard (metrics, positions, panic hedge, equity chart)
