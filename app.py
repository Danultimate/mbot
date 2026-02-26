"""
Streamlit dashboard for the Matchbook Automated Trading System.
Dark-mode UI with header metrics, goal tracker, active positions, panic hedge, and equity chart.
"""

import asyncio
from datetime import datetime

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import config
import db
from matchbook_api import MatchbookAPI


def _run_async(coro):
    """Run async coroutine from sync Streamlit context."""
    return asyncio.run(coro)


def _get_live_data():
    """Fetch live account and offers from Matchbook API. Returns None on error."""
    try:
        api = MatchbookAPI()
        data = _run_async(_fetch_live(api))
        _run_async(api.close())
        return data
    except Exception as e:
        st.error(f"API error: {e}")
        return None


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


def main():
    st.set_page_config(
        page_title="Matchbook Trading Dashboard",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    if "panic_in_progress" not in st.session_state:
        st.session_state["panic_in_progress"] = False

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

    # Data sources: DB for history, optional live API for real-time
    db.init_db()
    bankroll_row = db.get_current_bankroll()
    daily_roi = db.get_daily_roi_pct()
    open_positions = db.get_open_positions()
    equity_data = db.get_equity_curve()

    # Try live API for real-time metrics
    live = _get_live_data()
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

    # Header metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Current Bankroll (£)", f"£{balance:.2f}")
    with col2:
        roi_str = f"{daily_roi:.2f}%" if daily_roi is not None else "N/A"
        st.metric("Daily ROI (%)", roi_str)
    with col3:
        st.metric("Total Open Exposure (£)", f"£{exposure:.2f}")
    with col4:
        st.metric("Active Phase", phase_label)

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
    seen = set()
    for o in offers:
        key = (o.get("market-id"), o.get("runner-id"), o.get("side"))
        if key not in seen:
            seen.add(key)
            rows.append(
                {
                    "Market": o.get("market-name", ""),
                    "Selection": o.get("runner-name", ""),
                    "Side": o.get("side", "").capitalize(),
                    "Odds": o.get("odds") or o.get("decimal-odds"),
                    "Stake": o.get("stake") or o.get("remaining"),
                    "Status": o.get("status", ""),
                }
            )
    for p in open_positions:
        key = (p.get("market_id"), p.get("runner_id"), p.get("side"))
        if key not in seen:
            seen.add(key)
            rows.append(
                {
                    "Market": p.get("market_name", ""),
                    "Selection": p.get("runner_name", ""),
                    "Side": p.get("side", "").capitalize(),
                    "Odds": p.get("entry_odds"),
                    "Stake": p.get("entry_stake"),
                    "Status": "open",
                }
            )
    if rows:
        st.dataframe(rows, use_container_width=True)
    else:
        st.info("No active positions.")

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

    # Refresh
    if st.button("Refresh"):
        st.rerun()


if __name__ == "__main__":
    main()
