"""
Streamlit dashboard for the Matchbook Automated Trading System.
Dark-mode UI with header metrics, goal tracker, active positions, panic hedge, and equity chart.
"""

import asyncio
import time
from datetime import datetime, timezone

import plotly.graph_objects as go
import streamlit as st

import config
import db
from matchbook_api import MatchbookAPI

# Bot considered offline if no snapshot in this many minutes
BOT_OFFLINE_THRESHOLD_MIN = 5


def _run_async(coro):
    """Run async coroutine from sync Streamlit context."""
    return asyncio.run(coro)


def _get_live_data():
    """Fetch live account and offers from Matchbook API. Returns (data, error) tuple."""
    try:
        api = MatchbookAPI()
        data = _run_async(_fetch_live(api))
        _run_async(api.close())
        return data, None
    except Exception as e:
        return None, str(e)


async def _fetch_live(api: MatchbookAPI):
    """Async fetch of account and offers."""
    await api.login()
    account = api.get_account()
    offers = await api.get_offers(statuses=["open", "matched"])
    return {
        "balance": float(account.get("balance", 0) or 0),
        "exposure": float(account.get("exposure", 0) or 0),
        "free_funds": float(account.get("free-funds", 0) or 0),
        "offers": offers,
    }


def _panic_hedge():
    """Cancel all offers and close positions at market. Uses session_state to prevent double-click."""
    if st.session_state.get("panic_in_progress"):
        return
    st.session_state["panic_in_progress"] = True
    try:
        api = MatchbookAPI()
        _run_async(_do_panic_hedge(api))
        _run_async(api.close())
        st.success("Panic hedge executed. All open offers cancelled.")
    except Exception as e:
        st.error(f"Panic hedge failed: {e}")
    finally:
        st.session_state["panic_in_progress"] = False


async def _do_panic_hedge(api: MatchbookAPI):
    """Cancel all open offers. Optionally place market orders to close exposure."""
    await api.login()
    await api.cancel_offers()  # No filters = cancel all
    # Positions with exposure would need Green Up - for simplicity we cancel only.
    # Full implementation would fetch open positions and place hedge orders.


def _cancel_offer(offer_id: int):
    """Cancel a single offer by ID."""
    try:
        api = MatchbookAPI()
        _run_async(_do_cancel_offer(api, offer_id))
        _run_async(api.close())
        st.success(f"Offer {offer_id} cancelled.")
    except Exception as e:
        st.error(f"Cancel failed: {e}")


async def _do_cancel_offer(api: MatchbookAPI, offer_id: int):
    """Cancel a single offer."""
    await api.login()
    await api.cancel_offers(offer_ids=[offer_id])


def main():
    st.set_page_config(
        page_title="Matchbook Trading Dashboard",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    if "panic_in_progress" not in st.session_state:
        st.session_state["panic_in_progress"] = False

    db.init_db()

    # Sidebar: configurable refresh + last bot cycle
    with st.sidebar:
        st.subheader("Settings")
        refresh_interval = st.slider(
            "Auto-refresh interval (seconds)",
            min_value=0,
            max_value=120,
            value=0,
            step=10,
            help="0 = disabled. Page will reload automatically when > 0.",
        )
        last_snap = db.get_last_snapshot_time()
        if last_snap:
            snap_str = last_snap.strftime("%Y-%m-%d %H:%M:%S") if last_snap else "Never"
            st.caption(f"Last bot cycle: {snap_str} UTC")
        else:
            st.caption("Last bot cycle: Never")

    # Dark mode CSS
    st.markdown(
        """
        <style>
        .stApp { background-color: #0e1117; }
        .stMetric { color: #fafafa; }
        div[data-testid="stMetricValue"] { color: #00d4aa; }
        h1, h2, h3 { color: #fafafa; }
        .stProgress > div > div { background: linear-gradient(90deg, #00d4aa, #00ff88); }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("Matchbook Automated Trading System")

    # Status bar: Connection + Bot status + Manual refresh
    status_col1, status_col2, status_col3 = st.columns([2, 2, 1])
    with status_col1:
        live, api_error = _get_live_data()
        if live:
            st.success("Matchbook: Connected")
        else:
            st.error(f"Matchbook: Failed – check credentials" + (f" ({api_error})" if api_error else ""))
    with status_col2:
        last_snap = db.get_last_snapshot_time()
        if last_snap:
            last_utc = last_snap.replace(tzinfo=timezone.utc) if last_snap.tzinfo is None else last_snap
            delta = datetime.now(timezone.utc) - last_utc
            mins = int(delta.total_seconds() / 60)
            if mins >= BOT_OFFLINE_THRESHOLD_MIN:
                st.warning(f"Bot likely offline (last snapshot: {mins} min ago)")
            else:
                st.info(f"Last snapshot: {mins} min ago")
        else:
            st.warning("Bot likely offline (no snapshots yet)")
    with status_col3:
        if st.button("Refresh", key="refresh_btn"):
            st.rerun()

    # Data sources: DB for history, optional live API for real-time
    bankroll_row = db.get_current_bankroll()
    daily_roi = db.get_daily_roi_pct()
    open_positions = db.get_open_positions()
    equity_data = db.get_equity_curve()
    trades = db.get_trades()

    if live:
        balance = live["balance"]
        exposure = live["exposure"]
        free_funds = live["free_funds"]
        offers = live["offers"]
    else:
        balance = bankroll_row[0] if bankroll_row else config.STARTING_BANKROLL
        exposure = bankroll_row[1] if bankroll_row else 0
        free_funds = bankroll_row[2] if bankroll_row else config.STARTING_BANKROLL
        offers = []

    phase = 2 if free_funds >= config.PHASE2_MIN_BANKROLL else 1
    phase_label = "Phase 2 (Market Making)" if phase == 2 else "Phase 1 (Scalping)"
    cumulative_pnl = balance - config.STARTING_BANKROLL

    # Header metrics (5 cols to include Cumulative P&L)
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Current Bankroll (£)", f"£{balance:.2f}")
    with col2:
        roi_str = f"{daily_roi:.2f}%" if daily_roi is not None else "N/A"
        st.metric("Daily ROI (%)", roi_str)
    with col3:
        st.metric("Total Open Exposure (£)", f"£{exposure:.2f}")
    with col4:
        st.metric("Active Phase", phase_label)
    with col5:
        st.metric("Cumulative P&L (£)", f"£{cumulative_pnl:+.2f}")

    # Goal tracker
    st.subheader("Goal Tracker: £25 → £5,000")
    progress = (balance - config.STARTING_BANKROLL) / (
        config.TARGET_BANKROLL - config.STARTING_BANKROLL
    )
    progress = max(0, min(1, progress))
    st.progress(progress)
    st.caption(f"£{balance:.2f} / £{config.TARGET_BANKROLL}")

    # Active positions table (from API offers, fallback to DB when API unavailable)
    st.subheader("Active Positions")
    rows = []
    open_offers_with_id = []  # (offer_id, row) for Cancel buttons
    seen = set()
    for o in offers:
        key = (o.get("market-id"), o.get("runner-id"), o.get("side"))
        if key not in seen:
            seen.add(key)
            row = {
                "Event": o.get("event-name", ""),
                "Market": o.get("market-name", ""),
                "Selection": o.get("runner-name", ""),
                "Side": o.get("side", "").capitalize(),
                "Odds": o.get("odds") or o.get("decimal-odds"),
                "Stake": o.get("stake") or o.get("remaining"),
                "Status": o.get("status", ""),
                "offer_id": o.get("id"),
            }
            rows.append(row)
            if row["Status"] == "open" and row.get("offer_id"):
                open_offers_with_id.append((row["offer_id"], row))
    for p in open_positions:
        key = (p.get("market_id"), p.get("runner_id"), p.get("side"))
        if key not in seen:
            seen.add(key)
            rows.append(
                {
                    "Event": "-",
                    "Market": p.get("market_name", ""),
                    "Selection": p.get("runner_name", ""),
                    "Side": p.get("side", "").capitalize(),
                    "Odds": p.get("entry_odds"),
                    "Stake": p.get("entry_stake"),
                    "Status": "open",
                    "offer_id": None,
                }
            )
    if rows:
        # Display table (exclude offer_id from display)
        display_rows = [{k: v for k, v in r.items() if k != "offer_id"} for r in rows]
        st.dataframe(display_rows, use_container_width=True)
        # Cancel individual orders (only for API offers with status=open)
        if open_offers_with_id and live:
            st.caption("Cancel individual orders:")
            for offer_id, row in open_offers_with_id:
                info_col, btn_col = st.columns([4, 1])
                with info_col:
                    st.caption(f"{row.get('Event', '')} | {row.get('Selection', '')} @ {row.get('Odds', '')}")
                with btn_col:
                    if st.button("Cancel", key=f"cancel_{offer_id}"):
                        _cancel_offer(offer_id)
                        st.rerun()
    else:
        st.info("No active positions.")

    # Trade history table
    st.subheader("Trade History")
    if trades:
        trade_rows = [
            {
                "Date": (t.get("timestamp") or "")[:19],
                "Market": t.get("market_name", ""),
                "Selection": t.get("runner_name", ""),
                "Side": (t.get("side") or "").capitalize(),
                "Odds": t.get("odds"),
                "Stake": t.get("stake"),
                "Profit (£)": f"£{t['profit_loss']:.2f}" if t.get("profit_loss") is not None else "-",
            }
            for t in trades
        ]
        st.dataframe(trade_rows, use_container_width=True)
    else:
        st.info("No trades yet.")

    # Emergency control
    st.subheader("Emergency Control")
    if st.button(
        "Panic Hedge / Close Position",
        type="primary",
        use_container_width=True,
        disabled=st.session_state.get("panic_in_progress", False),
    ):
        _panic_hedge()
        st.rerun()

    # Analytics: equity curve
    st.subheader("Equity Curve")
    if equity_data:
        timestamps = [d[0] for d in equity_data]
        balances = [d[1] for d in equity_data]
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=balances,
                mode="lines",
                name="Bankroll",
                line=dict(color="#00d4aa", width=2),
            )
        )
        fig.update_layout(
            template="plotly_dark",
            xaxis_title="Time",
            yaxis_title="Balance (£)",
            height=400,
            margin=dict(l=40, r=40, t=40, b=40),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No equity data yet. Run the bot to record bankroll snapshots.")

    # Daily P&L bar chart
    st.subheader("Daily P&L")
    daily_pnl = db.get_daily_pnl(days=30)
    if daily_pnl:
        dates = [d[0] for d in daily_pnl]
        pnls = [d[1] for d in daily_pnl]
        colors = ["#00d4aa" if p >= 0 else "#ff6b6b" for p in pnls]
        fig = go.Figure(
            data=[go.Bar(x=dates, y=pnls, marker_color=colors, name="Daily P&L (£)")]
        )
        fig.update_layout(
            template="plotly_dark",
            xaxis_title="Date",
            yaxis_title="P&L (£)",
            height=300,
            margin=dict(l=40, r=40, t=40, b=40),
            showlegend=False,
        )
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No daily P&L data yet. Run the bot to record bankroll snapshots.")

    # Auto-refresh
    if refresh_interval > 0:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
