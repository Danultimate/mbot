"""
API Debug page - view all Matchbook API requests and responses.
"""

import streamlit as st

import config
import db

st.set_page_config(page_title="API Debug", page_icon="🔍", layout="wide")
st.title("API Debug – Request & Response Log")

st.caption(
    "Shows recent Matchbook API calls from the bot and dashboard. "
    "Use this to debug connection issues, inspect account data, and verify responses."
)

db.init_db()

# Test connection – optionally force re-login to see POST /security/session
force_relogin = st.checkbox(
    "Force re-login (clears saved session first)",
    value=False,
    help="Check this to trigger a fresh login. You'll see the POST to /security/session with account/balance in the response.",
    key="force_relogin",
)
if st.button("Test connection now", key="test_conn"):
    try:
        from matchbook_api import MatchbookAPI
        import asyncio

        async def _test():
            api = MatchbookAPI()
            if force_relogin:
                api._clear_session()
            await api.ensure_auth()
            account = api.get_account()
            await api.close()
            return account

        account = asyncio.run(_test())
        st.success("Connection OK")
        st.json(account)
        if force_relogin:
            st.info("Check the log below for the POST /security/session request and response (with account balance).")
    except Exception as e:
        st.error(f"Connection failed: {e}")
        import traceback
        st.code(traceback.format_exc())

st.divider()

# Fetch sports – discover correct sport-ids for get_events
st.subheader("Discover sport IDs")
st.caption(
    "Matchbook uses numeric sport IDs (e.g. Football may not be 1). "
    "Fetch the list and update your Sports selection in the sidebar."
)
if st.button("Fetch sports", key="fetch_sports"):
    try:
        from matchbook_api import MatchbookAPI
        import asyncio

        async def _fetch():
            api = MatchbookAPI()
            sports = await api.get_sports()
            await api.close()
            return sports

        sports = asyncio.run(_fetch())
        st.session_state["api_sports"] = sports
    except Exception as e:
        st.error(f"Fetch failed: {e}")
        import traceback
        st.code(traceback.format_exc())

if "api_sports" in st.session_state:
    sports = st.session_state["api_sports"]
    if sports:
        st.success(f"Found {len(sports)} sports (full list)")
        search = st.text_input("Filter by name (e.g. soccer, football)", key="sport_filter")
        rows = [{"id": s["id"], "name": s["name"], "type": s.get("type", "")} for s in sports]
        if search:
            q = search.lower()
            rows = [r for r in rows if q in str(r.get("name", "")).lower()]
            st.caption(f"Showing {len(rows)} matching '{search}'")
        st.table(rows)
    else:
        st.warning("No sports returned")

# Diagnose events – try without filters to see if API returns events
if st.button("Diagnose events (no sport/after filter)", key="diagnose_events"):
    try:
        from matchbook_api import MatchbookAPI
        import asyncio

        async def _diagnose():
            api = MatchbookAPI()
            events = await api.get_events(
                sport_ids=None,
                include_prices=True,
                price_depth=1,
                states="open,suspended",
                per_page=20,
                pre_match_only=False,
            )
            await api.close()
            return events

        events = asyncio.run(_diagnose())
        st.success(f"Got {len(events)} events (no sport/after filter)")
        if events:
            sport_ids = list({e.get("sport-id") for e in events if e.get("sport-id") is not None})
            st.info(f"Sport IDs in these events: {sport_ids}. Use these in your Sports selection.")
            sample = events[0]
            st.json({"sample_event": {"id": sample.get("id"), "name": sample.get("name"), "sport-id": sample.get("sport-id"), "markets_count": len(sample.get("markets", []))}})
        else:
            st.warning("0 events – API may be empty or auth/region issue.")
    except Exception as e:
        st.error(f"Diagnose failed: {e}")
        import traceback
        st.code(traceback.format_exc())

st.divider()

# Log display options
limit = st.slider("Show last N log entries", min_value=10, max_value=200, value=50, key="log_limit")
show_requests = st.checkbox("Show requests", value=True, key="show_req")
show_responses = st.checkbox("Show responses", value=True, key="show_res")

logs = db.get_api_logs(limit=limit)

if not logs:
    st.info("No API logs yet. The bot or dashboard will populate this when they make API calls.")
else:
    for log in reversed(logs):
        direction = log.get("direction", "")
        method = log.get("method", "")
        url = log.get("url", "")
        status = log.get("status")
        ts = (log.get("timestamp") or "")[:19]

        if direction == "request" and not show_requests:
            continue
        if direction == "response" and not show_responses:
            continue

        status_str = f" [{status}]" if status else ""
        header = f"**{ts}** {direction.upper()} {method} {url}{status_str}"
        if log.get("error"):
            header += f" — ERROR: {log['error'][:200]}"

        with st.expander(header):
            if log.get("request_body"):
                st.markdown("**Request:**")
                st.code(log["request_body"][:3000], language="json")
            if log.get("response_body"):
                st.markdown("**Response:**")
                st.code(log["response_body"][:3000], language="json")
            if log.get("error"):
                st.error(log["error"])
