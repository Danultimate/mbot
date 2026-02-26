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

            CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
            CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
            CREATE INDEX IF NOT EXISTS idx_bankroll_timestamp ON bankroll_snapshots(timestamp);
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
