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
