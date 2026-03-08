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

            CREATE TABLE IF NOT EXISTS hedge_cooldowns (
                market_id INTEGER NOT NULL,
                runner_id INTEGER NOT NULL,
                closed_at TEXT NOT NULL,
                PRIMARY KEY (market_id, runner_id)
            );

            CREATE TABLE IF NOT EXISTS closed_markets (
                market_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                closed_date TEXT NOT NULL,
                PRIMARY KEY (market_id, closed_date)
            );
            CREATE INDEX IF NOT EXISTS idx_closed_markets_date ON closed_markets(closed_date);

            CREATE TABLE IF NOT EXISTS blacklisted_markets (
                market_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                blacklisted_at TEXT NOT NULL,
                PRIMARY KEY (market_id)
            );
            CREATE INDEX IF NOT EXISTS idx_blacklisted_markets_market_id ON blacklisted_markets(market_id);

            CREATE TABLE IF NOT EXISTS paper_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id INTEGER NOT NULL,
                runner_id INTEGER NOT NULL,
                event_id INTEGER,
                event_name TEXT,
                market_name TEXT,
                runner_name TEXT,
                side TEXT NOT NULL,
                odds REAL NOT NULL,
                stake REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                phase INTEGER,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_paper_orders_status ON paper_orders(status);
            CREATE INDEX IF NOT EXISTS idx_paper_orders_market_runner ON paper_orders(market_id, runner_id);

            CREATE TABLE IF NOT EXISTS pending_hedge_confirmations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hedge_offer_id INTEGER NOT NULL,
                market_id INTEGER NOT NULL,
                runner_id INTEGER NOT NULL,
                side TEXT NOT NULL,
                stake REAL NOT NULL,
                odds REAL NOT NULL,
                market_name TEXT,
                runner_name TEXT,
                event_id INTEGER,
                position_id INTEGER,
                back_offer_id INTEGER,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_pending_hedge_offer_id ON pending_hedge_confirmations(hedge_offer_id);

            CREATE TABLE IF NOT EXISTS hedge_initiated (
                parent_offer_id INTEGER PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS hedged_selections (
                market_id INTEGER NOT NULL,
                runner_id INTEGER NOT NULL,
                hedged_at TEXT NOT NULL,
                PRIMARY KEY (market_id, runner_id)
            );

            CREATE TABLE IF NOT EXISTS phase2_leg_pairs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                back_offer_id INTEGER NOT NULL,
                lay_offer_id INTEGER NOT NULL,
                market_id INTEGER NOT NULL,
                runner_id INTEGER NOT NULL,
                event_id INTEGER,
                market_name TEXT,
                runner_name TEXT,
                event_name TEXT,
                stake REAL NOT NULL,
                back_odds REAL NOT NULL,
                lay_odds REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                leg_timer_started_at TEXT,
                matched_leg_side TEXT,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_phase2_leg_pairs_status ON phase2_leg_pairs(status);
        """)
        conn.commit()
        # Migration: add profit_loss to paper_trades (simulated fill logging)
        try:
            conn.execute("ALTER TABLE paper_trades ADD COLUMN profit_loss REAL")
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists
        # Migration: add event_name and reason to trades (unify with paper schema)
        for col in ("event_name", "reason"):
            try:
                conn.execute(f"ALTER TABLE trades ADD COLUMN {col} TEXT DEFAULT ''")
                conn.commit()
            except sqlite3.OperationalError:
                pass
        # Migration: add expected/slippage audit to trades
        for col in ("expected_profit", "slippage"):
            try:
                conn.execute(f"ALTER TABLE trades ADD COLUMN {col} REAL")
                conn.commit()
            except sqlite3.OperationalError:
                pass
        # Migration: add event_name to pending_hedge_confirmations
        try:
            conn.execute("ALTER TABLE pending_hedge_confirmations ADD COLUMN event_name TEXT DEFAULT ''")
            conn.commit()
        except sqlite3.OperationalError:
            pass
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
    expected_profit: Optional[float] = None,
    slippage: Optional[float] = None,
    event_name: str = "",
    reason: str = "",
) -> int:
    """Insert a trade record. Returns the new row id. Schema matches paper_trades (Event, Phase, Logic)."""
    conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO trades (timestamp, market_id, runner_id, market_name, runner_name,
               side, odds, stake, status, offer_id, phase, profit_loss, expected_profit, slippage, event_name, reason)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                expected_profit,
                slippage,
                event_name or "",
                reason or "",
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


def update_position_to_hedge_pending(position_id: int) -> None:
    """Mark position as hedge_pending (Child hedge placed; do not hedge again)."""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE positions SET status = 'hedge_pending' WHERE id = ?",
            (position_id,),
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
    """Return position dict for given offer_id, or None. Includes status for Execution Block."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT id, market_id, runner_id, market_name, runner_name, side, entry_odds, entry_stake, offer_id, status FROM positions WHERE offer_id = ?",
            (offer_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_position_by_id(position_id: int) -> Optional[dict]:
    """Return position dict for given id, or None. Includes status for Execution Block."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT id, market_id, runner_id, market_name, runner_name, side, entry_odds, entry_stake, offer_id, status FROM positions WHERE id = ?",
            (position_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def insert_hedge_initiated(parent_offer_id: int) -> None:
    """Hard Lock: record that we have initiated a Phase 2 hedge for this Parent. One Parent = ONE hedge, ever."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO hedge_initiated (parent_offer_id) VALUES (?)",
            (parent_offer_id,),
        )
        conn.commit()
    finally:
        conn.close()


def get_hedge_initiated_parent_ids() -> set[int]:
    """Return set of parent_offer_ids we have already initiated a hedge for."""
    conn = get_connection()
    try:
        rows = conn.execute("SELECT parent_offer_id FROM hedge_initiated").fetchall()
        return {int(r[0]) for r in rows if r[0] is not None}
    finally:
        conn.close()


def is_selection_hedged(market_id: int, runner_id: int) -> bool:
    """True if we have fired ANY hedge for this (market_id, runner_id). Permanent lock - no double exit."""
    conn = get_connection()
    try:
        mid = int(market_id)
        rid = int(runner_id)
        row = conn.execute(
            "SELECT 1 FROM hedged_selections WHERE market_id = ? AND runner_id = ?",
            (mid, rid),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def insert_hedged_selection(market_id: int, runner_id: int) -> None:
    """Lock this selection: we have fired a hedge. No further hedge may be placed for it."""
    conn = get_connection()
    try:
        mid = int(market_id)
        rid = int(runner_id)
        conn.execute(
            "INSERT OR IGNORE INTO hedged_selections (market_id, runner_id, hedged_at) VALUES (?, ?, ?)",
            (mid, rid, datetime.utcnow().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


# --- Phase 2 Leg Timer (adverse selection protection) ---


def insert_phase2_leg_pair(
    back_offer_id: int,
    lay_offer_id: int,
    market_id: int,
    runner_id: int,
    event_id: int,
    stake: float,
    back_odds: float,
    lay_odds: float,
    market_name: str = "",
    runner_name: str = "",
    event_name: str = "",
) -> int:
    """Record a Phase 2 Back+Lay pair for leg monitoring."""
    conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO phase2_leg_pairs
               (back_offer_id, lay_offer_id, market_id, runner_id, event_id, stake,
                back_odds, lay_odds, market_name, runner_name, event_name, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)""",
            (
                back_offer_id,
                lay_offer_id,
                market_id,
                runner_id,
                event_id,
                stake,
                back_odds,
                lay_odds,
                market_name or "",
                runner_name or "",
                event_name or "",
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_active_phase2_leg_pairs() -> list[dict]:
    """Return all active Phase 2 leg pairs for monitoring."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT id, back_offer_id, lay_offer_id, market_id, runner_id, event_id,
                      market_name, runner_name, event_name, stake, back_odds, lay_odds,
                      status, leg_timer_started_at, matched_leg_side
               FROM phase2_leg_pairs WHERE status = 'active'"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def update_phase2_leg_timer(pair_id: int, matched_leg_side: str) -> None:
    """Start the leg timer when first leg matches."""
    conn = get_connection()
    try:
        conn.execute(
            """UPDATE phase2_leg_pairs SET leg_timer_started_at = ?, matched_leg_side = ?
               WHERE id = ? AND status = 'active' AND leg_timer_started_at IS NULL""",
            (datetime.utcnow().isoformat(), matched_leg_side, pair_id),
        )
        conn.commit()
    finally:
        conn.close()


def mark_phase2_leg_pair_complete(pair_id: int, status: str = "complete") -> None:
    """Mark pair as complete (both matched or bailout done)."""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE phase2_leg_pairs SET status = ? WHERE id = ?",
            (status, pair_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_phase2_offer_ids() -> set[int]:
    """Return set of all offer IDs in active Phase 2 pairs (for hedge exclusion)."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT back_offer_id, lay_offer_id FROM phase2_leg_pairs WHERE status = 'active'"
        ).fetchall()
        ids = set()
        for r in rows:
            if r[0] is not None:
                ids.add(int(r[0]))
            if r[1] is not None:
                ids.add(int(r[1]))
        return ids
    finally:
        conn.close()


def get_all_tracked_offer_ids() -> set[int]:
    """Return set of all offer IDs currently tracked in local state (positions, pending hedges, hedge_initiated, phase2 pairs)."""
    conn = get_connection()
    try:
        ids = set()
        for row in conn.execute("SELECT offer_id FROM positions WHERE offer_id IS NOT NULL"):
            if row[0] is not None:
                ids.add(int(row[0]))
        for row in conn.execute("SELECT hedge_offer_id, back_offer_id FROM pending_hedge_confirmations"):
            if row[0] is not None:
                ids.add(int(row[0]))
            if row[1] is not None:
                ids.add(int(row[1]))
        for row in conn.execute("SELECT parent_offer_id FROM hedge_initiated"):
            if row[0] is not None:
                ids.add(int(row[0]))
        for row in conn.execute(
            "SELECT back_offer_id, lay_offer_id FROM phase2_leg_pairs WHERE status = 'active'"
        ):
            if row[0] is not None:
                ids.add(int(row[0]))
            if row[1] is not None:
                ids.add(int(row[1]))
        return ids
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


def has_open_position_for_runner(market_id: int, runner_id: int) -> bool:
    """Return True if we have an open position for this market/runner."""
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT 1 FROM positions WHERE market_id = ? AND runner_id = ? AND status = 'open'""",
            (market_id, runner_id),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def record_hedge_cooldown(market_id: int, runner_id: int) -> None:
    """Record that we hedged this selection; starts cooldown period."""
    conn = get_connection()
    try:
        now = datetime.utcnow().isoformat()
        conn.execute(
            """INSERT OR REPLACE INTO hedge_cooldowns (market_id, runner_id, closed_at)
               VALUES (?, ?, ?)""",
            (market_id, runner_id, now),
        )
        conn.commit()
    finally:
        conn.close()


def is_on_cooldown(market_id: int, runner_id: int, cooldown_sec: float = 60) -> bool:
    """Return True if we hedged this selection within cooldown_sec and cannot re-enter yet."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT closed_at FROM hedge_cooldowns WHERE market_id = ? AND runner_id = ?",
            (market_id, runner_id),
        ).fetchone()
        if not row or not row[0]:
            return False
        try:
            s = str(row[0]).replace("Z", "").split("+")[0].rstrip()
            closed = datetime.fromisoformat(s)
            if closed.tzinfo:
                closed = closed.replace(tzinfo=None)
            now = datetime.utcnow()
            elapsed = (now - closed).total_seconds()
            return 0 <= elapsed < cooldown_sec
        except (ValueError, TypeError):
            return False
    finally:
        conn.close()


def insert_closed_market(market_id: int, event_id: int = 0) -> None:
    """Record market as completed for today (One-and-Done: no re-entry this day)."""
    conn = get_connection()
    try:
        today = datetime.utcnow().date().isoformat()
        conn.execute(
            """INSERT OR IGNORE INTO closed_markets (market_id, event_id, closed_date)
               VALUES (?, ?, ?)""",
            (market_id, event_id, today),
        )
        conn.commit()
    finally:
        conn.close()


def is_market_closed_today(market_id: int) -> bool:
    """Return True if this market was completed (full cycle closed) today. One-and-Done rule."""
    conn = get_connection()
    try:
        today = datetime.utcnow().date().isoformat()
        row = conn.execute(
            "SELECT 1 FROM closed_markets WHERE market_id = ? AND closed_date = ?",
            (market_id, today),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def insert_blacklisted_market(market_id: int, event_id: int = 0) -> None:
    """Record market as blacklisted (successful Lay exit). Never re-enter this market."""
    conn = get_connection()
    try:
        now = datetime.utcnow().isoformat()
        conn.execute(
            """INSERT OR REPLACE INTO blacklisted_markets (market_id, event_id, blacklisted_at)
               VALUES (?, ?, ?)""",
            (market_id, event_id, now),
        )
        conn.commit()
    finally:
        conn.close()


def is_market_blacklisted(market_id: int) -> bool:
    """Return True if this market is blacklisted (had a successful Lay exit). Skip entry."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT 1 FROM blacklisted_markets WHERE market_id = ?",
            (market_id,),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


# --- Paper orders (Rule 1: unmatched check, Rule 3: simulated fill) ---


def get_paper_exposed_runners() -> set[tuple[int, int]]:
    """Return (market_id, runner_id) for all paper orders with status open OR matched."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT market_id, runner_id FROM paper_orders WHERE status IN ('open', 'matched')"
        ).fetchall()
        return {(int(r[0]), int(r[1])) for r in rows}
    finally:
        conn.close()


def insert_paper_order(
    market_id: int,
    runner_id: int,
    event_id: Optional[int],
    event_name: str,
    market_name: str,
    runner_name: str,
    side: str,
    odds: float,
    stake: float,
    phase: int,
) -> int:
    """Insert paper order as UNMATCHED. Returns row id."""
    conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO paper_orders (market_id, runner_id, event_id, event_name, market_name,
               runner_name, side, odds, stake, status, phase, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)""",
            (
                market_id,
                runner_id,
                event_id or 0,
                event_name or "",
                market_name or "",
                runner_name or "",
                side,
                odds,
                stake,
                phase,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_open_paper_orders() -> list[dict]:
    """Return all paper orders with status open (for simulated fill check)."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, market_id, runner_id, event_id, side, odds, stake, event_name, market_name, runner_name, phase "
            "FROM paper_orders WHERE status = 'open'"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def update_paper_order_matched(order_id: int) -> None:
    """Mark paper order as matched (simulated fill)."""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE paper_orders SET status = 'matched' WHERE id = ?",
            (order_id,),
        )
        conn.commit()
    finally:
        conn.close()


def insert_paper_trade_with_profit(
    event_name: str,
    market_name: str,
    runner_name: str,
    side: str,
    odds: float,
    stake: float,
    phase: int,
    reason: str,
    profit_loss: Optional[float] = None,
) -> int:
    """Insert paper trade with optional profit_loss (simulated fill)."""
    conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO paper_trades (timestamp, event_name, market_name, runner_name,
               side, odds, stake, phase, reason, profit_loss)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                profit_loss,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


# --- Pending hedge confirmations (Rule 2: Live source of truth) ---


def insert_pending_hedge_confirmation(
    hedge_offer_id: int,
    market_id: int,
    runner_id: int,
    side: str,
    stake: float,
    odds: float,
    market_name: str,
    runner_name: str,
    event_id: int,
    position_id: Optional[int],
    back_offer_id: Optional[int],
    event_name: str = "",
) -> int:
    """Record hedge order placed; will confirm via API poll before logging complete."""
    conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO pending_hedge_confirmations
               (hedge_offer_id, market_id, runner_id, side, stake, odds, market_name,
                runner_name, event_id, position_id, back_offer_id, event_name, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                hedge_offer_id,
                market_id,
                runner_id,
                side,
                stake,
                odds,
                market_name or "",
                runner_name or "",
                event_id,
                position_id,
                back_offer_id,
                event_name or "",
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_pending_hedge_confirmations() -> list[dict]:
    """Return all pending hedge confirmations (for API poll)."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT id, hedge_offer_id, market_id, runner_id, side, stake, odds,
                      market_name, runner_name, event_id, position_id, back_offer_id, event_name
               FROM pending_hedge_confirmations"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def delete_pending_hedge_confirmation(pending_id: int) -> None:
    """Remove pending confirmation after processing."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM pending_hedge_confirmations WHERE id = ?", (pending_id,))
        conn.commit()
    finally:
        conn.close()


def get_trades(limit: int = 100) -> list[dict]:
    """Return trade history. Schema matches paper_trades (Event, Phase, Logic)."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT timestamp, event_name, market_name, runner_name, side, odds, stake, phase, reason,
                      profit_loss, expected_profit, slippage
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
    """Clear all paper trades and paper orders (for testing)."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM paper_trades")
        conn.execute("DELETE FROM paper_orders")
        conn.commit()
    finally:
        conn.close()


def get_paper_trades(limit: int = 50) -> list[dict]:
    """Return recent paper trades for display. Includes profit_loss for simulated fills."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT timestamp, event_name, market_name, runner_name, side, odds, stake, phase, reason, profit_loss
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
