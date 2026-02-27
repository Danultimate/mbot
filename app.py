"""
Streamlit dashboard for the Matchbook Automated Trading System.
Dark-mode UI with header metrics, goal tracker, active positions, panic hedge, and equity chart.
Optional password protection via DASHBOARD_PASSWORD env var.
"""

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Optional

import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

import alerts
import config
import db
from matchbook_api import MatchbookAPI

load_dotenv()

# Bot considered offline if no snapshot in this many minutes
BOT_OFFLINE_THRESHOLD_MIN = 5


def _get_dashboard_password() -> Optional[str]:
    """Return configured dashboard password, or None if auth disabled."""
    pwd = os.getenv("DASHBOARD_PASSWORD", "").strip()
    return pwd if pwd else None


def _check_auth() -> bool:
    """Return True if user is authenticated. Show login form if not."""
    required = _get_dashboard_password()
    if not required:
        return True  # No password set, allow access

    if st.session_state.get("dashboard_authenticated"):
        return True

    st.title("Matchbook Trading Dashboard")
    st.markdown("Enter the dashboard password to continue.")
    pwd = st.text_input("Password", type="password", key="dashboard_pwd")
    if st.button("Log in", key="dashboard_login"):
        if pwd == required:
            st.session_state["dashboard_authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False


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


def _parse_account_balance(account: dict) -> tuple[float, float, float]:
    """Parse balance, exposure, free_funds from account. Handles various key names."""
    def _get(key: str, alt: str = "") -> float:
        v = account.get(key) or account.get(alt)
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0
    return (
        _get("balance", "account-balance"),
        _get("exposure"),
        _get("free-funds", "free_funds"),
    )


async def _fetch_live(api: MatchbookAPI):
    """Async fetch of account and offers. Uses persisted session if valid."""
    await api.ensure_auth()
    account = api.get_account()
    offers = await api.get_offers(statuses=["open", "matched"])
    balance, exposure, free_funds = _parse_account_balance(account)
    return {
        "balance": balance,
        "exposure": exposure,
        "free_funds": free_funds,
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
        alerts.send_alert("Panic hedge executed. Open offers cancelled and matched positions hedged.", "panic_hedge")
        st.success("Panic hedge executed. Open offers cancelled and matched positions hedged.")
    except Exception as e:
        st.error(f"Panic hedge failed: {e}")
    finally:
        st.session_state["panic_in_progress"] = False


async def _do_panic_hedge(api: MatchbookAPI):
    """Cancel all open offers and Green Up all matched positions."""
    await api.ensure_auth()
    await api.cancel_offers()  # No filters = cancel all open
    # Hedge all matched positions (Back with Lay, Lay with Back)
    from bot import hedge_all_matched_positions
    await hedge_all_matched_positions(api, hedge_all=True)


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
    await api.ensure_auth()
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

    if not _check_auth():
        st.stop()

    db.init_db()

    # Sidebar: bot on/off, paper trading, stop-loss, configurable refresh
    with st.sidebar:
        if _get_dashboard_password():
            if st.button("Log out", key="dashboard_logout"):
                st.session_state.pop("dashboard_authenticated", None)
                st.rerun()
        st.subheader("Bot Control")
        bot_enabled = db.get_bot_enabled()
        new_state = st.toggle("Trading enabled", value=bot_enabled, key="bot_toggle")
        if new_state != bot_enabled:
            db.set_bot_enabled(new_state)
            st.success("Bot " + ("enabled" if new_state else "paused") + ". Changes apply on next cycle.")
            st.rerun()

        pre_match_only = db.get_pre_match_only()
        pre_match_state = st.toggle("Pre-match only (no in-play)", value=pre_match_only, key="pre_match_toggle")
        if pre_match_state != pre_match_only:
            db.set_pre_match_only(pre_match_state)
            st.success("Pre-match only " + ("on" if pre_match_state else "off") + ".")
            st.rerun()

        close_before_min = db.get_close_before_start_minutes()
        new_close_min = st.number_input(
            "Close before start (minutes)",
            min_value=1,
            max_value=60,
            value=int(close_before_min),
            step=1,
            key="close_before_start",
            help="When pre-match only: close orders for events starting within this many minutes.",
        )
        if new_close_min != close_before_min:
            db.set_close_before_start_minutes(float(new_close_min))
            st.rerun()

        paper_trading = db.get_paper_trading()
        paper_state = st.toggle("Paper trading", value=paper_trading, key="paper_toggle")
        if paper_state != paper_trading:
            db.set_paper_trading(paper_state)
            st.success("Paper trading " + ("on" if paper_state else "off") + ".")
            st.rerun()

        stop_loss_pct = db.get_daily_stop_loss_pct()
        new_stop_loss = st.slider(
            "Daily stop-loss (%)",
            min_value=1,
            max_value=50,
            value=int(stop_loss_pct),
            step=1,
            key="stop_loss_slider",
        )
        if new_stop_loss != stop_loss_pct:
            db.set_daily_stop_loss_pct(float(new_stop_loss))
            st.rerun()

        stop_loss_triggered = db.get_stop_loss_triggered()
        if stop_loss_triggered:
            st.warning("Daily stop-loss triggered. Trading paused.")
            if st.button("Clear stop-loss & resume", key="clear_stop_loss"):
                db.clear_stop_loss()
                st.success("Stop-loss cleared. Trading can resume.")
                st.rerun()

        commission_rate = db.get_commission_rate()
        commission_pct = int(commission_rate * 100)
        new_commission = st.select_slider(
            "Matchbook commission (%)",
            options=[2, 3, 4, 5],
            value=commission_pct if commission_pct in (2, 3, 4, 5) else 2,
            format_func=lambda x: f"{x}%",
            key="commission_slider",
            help="2% UK/ROI, 4% other regions. Used for gross ROI target and profit projections.",
        )
        if new_commission / 100.0 != commission_rate:
            db.set_commission_rate(new_commission / 100.0)
            st.rerun()

        with st.expander("Commission impact"):
            example_gross = 100.0
            example_net = config.net_profit_after_commission(
                example_gross, commission_rate=db.get_commission_rate()
            )
            st.caption(
                f"£{example_gross:.0f} gross profit → £{example_net:.0f} net "
                f"(after {int(db.get_commission_rate()*100)}% commission)"
            )

        with st.expander("Alerts"):
            status = alerts.get_channel_status()
            for name, msg in status.items():
                st.caption(f"{name.capitalize()}: {msg}")
            if any(alerts.get_configured_channels().values()):
                if st.button("Test alert", key="test_alert"):
                    alerts.send_alert("This is a test alert from the dashboard.", "test")
                    st.success("Test alert sent to configured channels.")
            else:
                st.caption(
                    "Add ALERT_* vars to your .env file to enable. "
                    "See .env.example for Telegram, Discord, and email options."
                )

        st.subheader("Market / Sport")
        # Sport IDs: Matchbook uses various IDs - 1=Football common; verify via API /edge/rest/lookups/sports
        sport_options = {
            "Football": [1],
            "Tennis": [2],
            "Horse Racing": [7],
            "Politics": [6],
        }
        current_sports = db.get_sport_ids()
        sport_labels = [k for k, v in sport_options.items() if any(x in current_sports for x in v)]
        selected_sports = st.multiselect(
            "Sports",
            options=list(sport_options.keys()),
            default=sport_labels if sport_labels else ["Football"],
            key="sport_select",
        )
        sport_ids = []
        for s in selected_sports:
            sport_ids.extend(sport_options.get(s, []))
        if set(sport_ids) != set(current_sports):
            db.set_sport_ids(sport_ids if sport_ids else [1])
            st.rerun()

        market_options = {
            "Match Odds": "one_x_two",
            "Money Line": "money_line",
            "Over/Under 2.5": "over_under_25",
        }
        current_markets = db.get_market_types()
        market_labels = [k for k, v in market_options.items() if v in current_markets]
        selected_markets = st.multiselect(
            "Market types",
            options=list(market_options.keys()),
            default=market_labels if market_labels else ["Match Odds", "Over/Under 2.5"],
            key="market_select",
        )
        market_types = [market_options[m] for m in selected_markets if m in market_options]
        if set(market_types) != set(current_markets):
            db.set_market_types(market_types if market_types else ["one_x_two"])
            st.rerun()

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
    status_col1, status_col2, status_col3, status_col4 = st.columns([2, 2, 1, 1])
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
        if db.get_stop_loss_triggered():
            st.error("Stop-loss triggered")
        elif db.get_paper_trading():
            st.info("Paper trading")
        elif db.get_bot_enabled():
            st.success("Bot: Trading")
        else:
            st.warning("Bot: Paused")
    with status_col4:
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
        data_source = "live"
    else:
        balance = bankroll_row[0] if bankroll_row else config.STARTING_BANKROLL
        exposure = bankroll_row[1] if bankroll_row else 0
        free_funds = bankroll_row[2] if bankroll_row else config.STARTING_BANKROLL
        offers = []
        data_source = "cached" if bankroll_row else "default"

    phase = 2 if free_funds >= config.PHASE2_MIN_BANKROLL else 1
    phase_label = "Phase 2 (Market Making)" if phase == 2 else "Phase 1 (Scalping)"
    cumulative_pnl = balance - config.STARTING_BANKROLL

    # Header metrics (5 cols to include Cumulative P&L)
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Current Bankroll (£)", f"£{balance:.2f}")
        if data_source != "live":
            st.caption(f"Source: {data_source} (API failed?)")
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
    gross_target = config.gross_roi_target_pct(commission_rate=db.get_commission_rate())
    st.caption(
        f"£{balance:.2f} / £{config.TARGET_BANKROLL} • "
        f"Target: {config.DAILY_ROI_TARGET_PCT}% net daily ROI "
        f"(≈{gross_target:.1f}% gross after {int(db.get_commission_rate()*100)}% commission)"
    )

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
    comm_pct = int(db.get_commission_rate() * 100)
    st.caption(
        f"Profit shown is net of Matchbook commission ({comm_pct}% on winnings). "
        "Balance from API is already post-commission."
    )
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

    # Paper trading activity (simulated orders when paper mode is on)
    st.subheader("Paper Trading Activity")
    if db.get_paper_trading():
        paper_trades = db.get_paper_trades()
        if paper_trades:
            pt_rows = [
                {
                    "Time": (t.get("timestamp") or "")[:19],
                    "Event": t.get("event_name", ""),
                    "Market": t.get("market_name", ""),
                    "Selection": t.get("runner_name", ""),
                    "Side": (t.get("side") or "").capitalize(),
                    "Odds": t.get("odds"),
                    "Stake": t.get("stake"),
                    "Phase": t.get("phase"),
                    "Logic": t.get("reason", ""),
                }
                for t in paper_trades
            ]
            st.dataframe(pt_rows, use_container_width=True)
            st.caption("Simulated orders the bot would have placed. No real money at risk.")
            if st.button("Clear paper trade history", key="clear_paper"):
                db.clear_paper_trades()
                st.rerun()
        else:
            st.info("No paper trades yet. Enable paper trading and let the bot run a cycle.")
    else:
        st.caption("Enable paper trading in the sidebar to see simulated orders.")

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
