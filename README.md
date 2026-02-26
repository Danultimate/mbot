# Matchbook Automated Trading System

A local automated trading system for the Matchbook Exchange with a headless Python bot and Streamlit dashboard. Target: compound £25 to £5,000 at 5% daily ROI.

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

- **Bot**: Runs every 60 seconds, shares SQLite DB with dashboard
- **Dashboard**: http://localhost:8501

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
