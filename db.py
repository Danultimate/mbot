"""
SQLite database module for the Matchbook Trading System.
Stores trade history, active positions, and daily bankroll snapshots.
"""

import sqlite3
from datetime import datetime
from typing import Optional

import config


def get_connection():
    """Return a connection to the SQLite database."""
    return sqlite3.connect(config.DB_PATH)


def init_db() -> None:
    """
    Initialize SQLite tables: trades, positions, bankroll_snapshots.
    Idempotent - safe to call multiple times.
    """
    conn = get_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                market_id INTEGER,
                runner_id INTEGER,
                market_name TEXT,
                runner_name TEXT,
                side TEXT NOT NULL,
                odds REAL NOT NULL,
                stake REAL NOT NULL,
                status TEXT,
                offer_id INTEGER,
                phase INTEGER,
                profit_loss REAL
            );

            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id INTEGER,
                runner_id INTEGER,
                market_name TEXT,
                runner_name TEXT,
                side TEXT NOT NULL,
                entry_odds REAL NOT NULL,
                entry_stake REAL NOT NULL,
                entry_time TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                offer_id INTEGER,
                closed_at TEXT,
                profit_loss REAL
            );

            CREATE TABLE IF NOT EXISTS bankroll_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                balance REAL NOT NULL,
                exposure REAL NOT NULL,
                free_funds REAL NOT NULL,
                daily_roi_pct REAL
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            INSERT OR IGNORE INTO settings (key, value) VALUES ('bot_enabled', '1');
            INSERT OR IGNORE INTO settings (key, value) VALUES ('paper_trading', '0');

            CREATE TABLE IF NOT EXISTS api_session (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                session_token TEXT,
                account_json TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_name TEXT,
                market_name TEXT,
                runner_name TEXT,
                side TEXT NOT NULL,
                odds REAL NOT NULL,
                stake REAL NOT NULL,
                phase INTEGER,
                reason TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
            CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
            CREATE INDEX IF NOT EXISTS idx_bankroll_timestamp ON bankroll_snapshots(timestamp);
            CREATE TABLE IF NOT EXISTS api_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                direction TEXT NOT NULL,
                method TEXT,
                url TEXT,
                status INTEGER,
                request_body TEXT,
                response_body TEXT,
                error TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_paper_trades_timestamp ON paper_trades(timestamp);
            CREATE INDEX IF NOT EXISTS idx_api_logs_timestamp ON api_logs(timestamp);
        """)
        conn.commit()
    finally:
        conn.close()


def insert_trade(
    market_id: int,
    runner_id: int,
    market_name: str,
    runner_name: str,
    side: str,
    odds: float,
    stake: float,
    status: str,
    offer_id: Optional[int] = None,
    phase: Optional[int] = None,
    profit_loss: Optional[float] = None,
) -> int:
    """Insert a trade record. Returns the new row id."""
    conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO trades (timestamp, market_id, runner_id, market_name, runner_name,
               side, odds, stake, status, offer_id, phase, profit_loss)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.utcnow().isoformat(),
                market_id,
                runner_id,
                market_name or "",
                runner_name or "",
                side,
                odds,
                stake,
                status,
                offer_id,
                phase,
                profit_loss,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def insert_position(
    market_id: int,
    runner_id: int,
    market_name: str,
    runner_name: str,
    side: str,
    entry_odds: float,
    entry_stake: float,
    offer_id: Optional[int] = None,
) -> int:
    """Insert an open position. Returns the new row id."""
    conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO positions (market_id, runner_id, market_name, runner_name,
               side, entry_odds, entry_stake, entry_time, status, offer_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)""",
            (
                market_id,
                runner_id,
                market_name or "",
                runner_name or "",
                side,
                entry_odds,
                entry_stake,
                datetime.utcnow().isoformat(),
                offer_id,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_position(
    position_id: int,
    status: str = "closed",
    profit_loss: Optional[float] = None,
) -> None:
    """Update a position (e.g. mark as closed)."""
    conn = get_connection()
    try:
        conn.execute(
            """UPDATE positions SET status = ?, closed_at = ?, profit_loss = ?
               WHERE id = ?""",
            (status, datetime.utcnow().isoformat(), profit_loss, position_id),
        )
        conn.commit()
    finally:
        conn.close()


def insert_bankroll_snapshot(
    balance: float,
    exposure: float,
    free_funds: float,
    daily_roi_pct: Optional[float] = None,
) -> int:
    """Insert a bankroll snapshot. Returns the new row id."""
    conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO bankroll_snapshots (timestamp, balance, exposure, free_funds, daily_roi_pct)
               VALUES (?, ?, ?, ?, ?)""",
            (datetime.utcnow().isoformat(), balance, exposure, free_funds, daily_roi_pct),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_equity_curve() -> list[tuple[str, float]]:
    """
    Return list of (timestamp, balance) for the equity curve chart.
    Uses bankroll_snapshots ordered by timestamp.
    """
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT timestamp, balance FROM bankroll_snapshots ORDER BY timestamp"""
        ).fetchall()
        return [(r[0], r[1]) for r in rows]
    finally:
        conn.close()


def get_current_bankroll() -> Optional[tuple[float, float, float]]:
    """
    Return the most recent bankroll snapshot as (balance, exposure, free_funds).
    Returns None if no snapshots exist.
    """
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT balance, exposure, free_funds FROM bankroll_snapshots
               ORDER BY timestamp DESC LIMIT 1"""
        ).fetchone()
        return tuple(row) if row else None
    finally:
        conn.close()


def get_position_by_offer_id(offer_id: int) -> Optional[dict]:
    """Return position dict for given offer_id, or None."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT id, market_id, runner_id, market_name, runner_name, side, entry_odds, entry_stake, offer_id FROM positions WHERE offer_id = ?",
            (offer_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_open_positions() -> list[dict]:
    """Return list of open positions as dicts."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT id, market_id, runner_id, market_name, runner_name,
                      side, entry_odds, entry_stake, entry_time, offer_id
               FROM positions WHERE status = 'open'"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_trades(limit: int = 100) -> list[dict]:
    """Return trade history (date, market, selection, side, odds, stake, profit_loss)."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT timestamp, market_name, runner_name, side, odds, stake, profit_loss
               FROM trades ORDER BY timestamp DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_last_snapshot_time() -> Optional[datetime]:
    """Return timestamp of most recent bankroll snapshot, or None."""
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT timestamp FROM bankroll_snapshots ORDER BY timestamp DESC LIMIT 1"""
        ).fetchone()
        if row and row[0]:
            try:
                return datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                return None
        return None
    finally:
        conn.close()


def get_bot_enabled() -> bool:
    """Return True if bot is enabled, False if paused."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'bot_enabled'"
        ).fetchone()
        return (row and row[0] and str(row[0]).lower() in ("1", "true", "yes")) if row else True
    finally:
        conn.close()


def get_api_session() -> Optional[tuple[str, str]]:
    """Return (session_token, account_json) or None."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT session_token, account_json FROM api_session WHERE id = 1"
        ).fetchone()
        return (row[0], row[1]) if row and row[0] else None
    finally:
        conn.close()


def set_api_session(session_token: str, account_json: str) -> None:
    """Persist session token and account for reuse."""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO api_session (id, session_token, account_json, updated_at)
               VALUES (1, ?, ?, ?)""",
            (session_token, account_json, datetime.utcnow().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def clear_api_session() -> None:
    """Clear persisted session (e.g. on logout or 401)."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM api_session WHERE id = 1")
        conn.commit()
    finally:
        conn.close()


def get_daily_start_balance() -> Optional[float]:
    """Return first balance of today from bankroll_snapshots, or None."""
    conn = get_connection()
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        row = conn.execute(
            """SELECT balance FROM bankroll_snapshots
               WHERE date(timestamp) = date(?)
               ORDER BY timestamp ASC LIMIT 1""",
            (today,),
        ).fetchone()
        return float(row[0]) if row and row[0] is not None else None
    finally:
        conn.close()


def get_stop_loss_triggered() -> bool:
    """True if daily stop-loss was triggered today."""
    conn = get_connection()
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'stop_loss_triggered_date'"
        ).fetchone()
        return (row and row[0] == today) if row else False
    finally:
        conn.close()


def set_stop_loss_triggered() -> None:
    """Mark today as stop-loss triggered."""
    conn = get_connection()
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('stop_loss_triggered_date', ?)",
            (today,),
        )
        conn.commit()
    finally:
        conn.close()


def get_daily_stop_loss_pct() -> float:
    """Return configured daily stop-loss % (from settings or config default)."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'daily_stop_loss_pct'"
        ).fetchone()
        if row and row[0]:
            try:
                return float(row[0])
            except (ValueError, TypeError):
                pass
        return config.DAILY_STOP_LOSS_PCT
    finally:
        conn.close()


def set_daily_stop_loss_pct(pct: float) -> None:
    """Set daily stop-loss % in settings."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('daily_stop_loss_pct', ?)",
            (str(pct),),
        )
        conn.commit()
    finally:
        conn.close()


def get_commission_rate() -> float:
    """Return commission rate (0–1, e.g. 0.02 for 2%). From settings or config."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'commission_rate'"
        ).fetchone()
        if row and row[0]:
            try:
                return float(row[0])
            except (ValueError, TypeError):
                pass
        return config.COMMISSION_RATE
    finally:
        conn.close()


def set_commission_rate(rate: float) -> None:
    """Set commission rate (0–1). UK/ROI: 0.02, other regions: 0.04."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('commission_rate', ?)",
            (str(rate),),
        )
        conn.commit()
    finally:
        conn.close()


def clear_stop_loss() -> None:
    """Clear stop-loss so trading can resume."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM settings WHERE key = 'stop_loss_triggered_date'")
        conn.commit()
    finally:
        conn.close()


def get_paper_trading() -> bool:
    """Return True if paper trading mode is enabled."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'paper_trading'"
        ).fetchone()
        return (row and row[0] and str(row[0]).lower() in ("1", "true", "yes")) if row else False
    finally:
        conn.close()


def set_paper_trading(enabled: bool) -> None:
    """Enable or disable paper trading mode."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('paper_trading', ?)",
            ("1" if enabled else "0",),
        )
        conn.commit()
    finally:
        conn.close()


def insert_paper_trade(
    event_name: str,
    market_name: str,
    runner_name: str,
    side: str,
    odds: float,
    stake: float,
    phase: int,
    reason: str = "",
) -> int:
    """Insert a paper trade (simulated order). Returns row id."""
    conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO paper_trades (timestamp, event_name, market_name, runner_name,
               side, odds, stake, phase, reason)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.utcnow().isoformat(),
                event_name or "",
                market_name or "",
                runner_name or "",
                side,
                odds,
                stake,
                phase,
                reason or "",
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def clear_paper_trades() -> None:
    """Clear all paper trades (for testing)."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM paper_trades")
        conn.commit()
    finally:
        conn.close()


def get_paper_trades(limit: int = 50) -> list[dict]:
    """Return recent paper trades for display."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT timestamp, event_name, market_name, runner_name, side, odds, stake, phase, reason
               FROM paper_trades ORDER BY timestamp DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_sport_ids() -> list[int]:
    """Return sport IDs from settings, or config default."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'sport_ids'"
        ).fetchone()
        if row and row[0]:
            try:
                return [int(x.strip()) for x in str(row[0]).split(",") if x.strip()]
            except (ValueError, TypeError):
                pass
        return config.SPORT_IDS
    finally:
        conn.close()


def set_sport_ids(ids: list[int]) -> None:
    """Store sport IDs in settings."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('sport_ids', ?)",
            (",".join(str(i) for i in ids),),
        )
        conn.commit()
    finally:
        conn.close()


def get_market_types() -> list[str]:
    """Return market types from settings, or config default."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'market_types'"
        ).fetchone()
        if row and row[0]:
            return [x.strip() for x in str(row[0]).split(",") if x.strip()]
        return config.MARKET_TYPES
    finally:
        conn.close()


def get_close_before_start_minutes() -> float:
    """Minutes before event start to close orders. From settings or config."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'close_before_start_minutes'"
        ).fetchone()
        if row and row[0]:
            try:
                return float(row[0])
            except (ValueError, TypeError):
                pass
        return config.CLOSE_BEFORE_START_MINUTES
    finally:
        conn.close()


def set_close_before_start_minutes(minutes: float) -> None:
    """Set minutes before event start to close orders."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('close_before_start_minutes', ?)",
            (str(minutes),),
        )
        conn.commit()
    finally:
        conn.close()


def get_pre_match_only() -> bool:
    """Return True if bot should only trade pre-match (exclude in-play)."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'pre_match_only'"
        ).fetchone()
        if row and row[0] is not None:
            return str(row[0]).lower() in ("1", "true", "yes")
        return config.PRE_MATCH_ONLY
    finally:
        conn.close()


def set_pre_match_only(enabled: bool) -> None:
    """Enable or disable pre-match-only filter."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('pre_match_only', ?)",
            ("1" if enabled else "0",),
        )
        conn.commit()
    finally:
        conn.close()


def set_market_types(types: list[str]) -> None:
    """Store market types in settings."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('market_types', ?)",
            (",".join(types),),
        )
        conn.commit()
    finally:
        conn.close()


def set_bot_enabled(enabled: bool) -> None:
    """Enable or disable the bot."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('bot_enabled', ?)",
            ("1" if enabled else "0",),
        )
        conn.commit()
    finally:
        conn.close()


def get_daily_pnl(days: int = 30) -> list[tuple[str, float]]:
    """
    Return list of (date, pnl) for daily P&L chart.
    P&L = last balance of day - first balance of day (intraday change).
    """
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT date(timestamp) as d, 
                      MIN(balance) as first_bal, MAX(balance) as last_bal
               FROM bankroll_snapshots
               GROUP BY date(timestamp)
               ORDER BY d ASC LIMIT ?""",
            (days,),
        ).fetchall()
        return [(r[0], r[2] - r[1]) for r in rows] if rows else []
    finally:
        conn.close()


def get_daily_roi_pct() -> Optional[float]:
    """
    Compute daily ROI % from today's first snapshot vs latest.
    Returns None if insufficient data.
    """
    conn = get_connection()
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        first = conn.execute(
            """SELECT balance FROM bankroll_snapshots
               WHERE date(timestamp) = date(?)
               ORDER BY timestamp ASC LIMIT 1""",
            (today,),
        ).fetchone()
        latest = conn.execute(
            """SELECT balance FROM bankroll_snapshots
               WHERE date(timestamp) = date(?)
               ORDER BY timestamp DESC LIMIT 1""",
            (today,),
        ).fetchone()
        if first and latest and first[0] and first[0] > 0:
            return ((latest[0] - first[0]) / first[0]) * 100
        return None
    finally:
        conn.close()


def insert_api_log(
    direction: str,
    method: str = "",
    url: str = "",
    status: Optional[int] = None,
    request_body: Optional[str] = None,
    response_body: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    """Log an API request or response for debugging."""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO api_logs (timestamp, direction, method, url, status, request_body, response_body, error)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.utcnow().isoformat(),
                direction,
                method or "",
                url or "",
                status,
                (request_body or "")[:10000] if request_body else None,
                (response_body or "")[:10000] if response_body else None,
                (error or "")[:2000] if error else None,
            ),
        )
        conn.commit()
        # Keep only last 500 logs
        conn.execute("DELETE FROM api_logs WHERE id NOT IN (SELECT id FROM api_logs ORDER BY id DESC LIMIT 500)")
        conn.commit()
    finally:
        conn.close()


def get_api_logs(limit: int = 100) -> list[dict]:
    """Return recent API logs for the debug page."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT id, timestamp, direction, method, url, status, request_body, response_body, error
               FROM api_logs ORDER BY id DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
