"""
Asynchronous Matchbook Exchange API wrapper.
Handles authentication (session tokens, expiry), market data, and order execution.
Session is persisted to DB - login only when token expired (401) or missing.
"""

import asyncio
import json
import logging
import os
import time
from typing import Any, Optional

import aiohttp
from dotenv import load_dotenv

import alerts
import config
import db

load_dotenv()

logger = logging.getLogger(__name__)


class MarketSuspendedError(Exception):
    """Raised when the market is suspended (e.g. during goal/VAR in live football)."""

    pass


class MatchbookAPIError(Exception):
    """Generic API error with status and message."""

    def __init__(self, status: int, message: str, body: Optional[str] = None):
        self.status = status
        self.message = message
        self.body = body
        super().__init__(f"Matchbook API error {status}: {message}")


class MatchbookAPI:
    """
    Async API client for Matchbook Exchange.
    Uses bpapi for auth, edge/rest for events and offers.
    """

    def __init__(self):
        self._session_token: Optional[str] = None
        self._account: Optional[dict] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._rate_limit_delay = config.RATE_LIMIT_DELAY_MS / 1000.0
        self._timeout = aiohttp.ClientTimeout(total=config.API_TIMEOUT_SEC)
        self._load_persisted_session()

    def _load_persisted_session(self) -> None:
        """Load session token and account from DB if available."""
        try:
            row = db.get_api_session()
            if row:
                token, account_json = row
                if token:
                    self._session_token = token
                    if account_json:
                        try:
                            self._account = json.loads(account_json)
                        except json.JSONDecodeError:
                            pass
        except Exception as e:
            logger.debug("Could not load persisted session: %s", e)

    def _save_session(self) -> None:
        """Persist session to DB for reuse across requests/cycles."""
        try:
            if self._session_token and self._account is not None:
                db.set_api_session(
                    self._session_token,
                    json.dumps(self._account) if self._account else "{}",
                )
        except Exception as e:
            logger.debug("Could not persist session: %s", e)

    def _clear_session(self) -> None:
        """Clear in-memory and persisted session."""
        self._session_token = None
        self._account = None
        try:
            db.clear_api_session()
        except Exception:
            pass

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Create or return existing aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    def _auth_headers(self) -> dict:
        """Headers including session token for authenticated requests."""
        h = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
        }
        if self._session_token:
            h["session-token"] = self._session_token
        return h

    async def _rate_limit(self) -> None:
        """Sleep to avoid breaching API rate limits."""
        await asyncio.sleep(self._rate_limit_delay)

    async def _check_suspended(self, status: int, body: str) -> None:
        """
        Check if response indicates market suspended.
        HTTP 400/403 with 'suspended' or 'Market Suspended' in body.
        """
        if status in (400, 403) and body:
            body_lower = body.lower()
            if "suspended" in body_lower or "market suspended" in body_lower:
                raise MarketSuspendedError("Market is suspended")

    async def login(self) -> dict:
        """
        Login to Matchbook and obtain session token.
        POST to bpapi/rest/security/session with username/password.
        Response includes session-token and account (balance, exposure, free-funds).
        Persists session to DB for reuse - only needed when token expired or missing.
        """
        username = os.getenv("MATCHBOOK_USER")
        password = os.getenv("MATCHBOOK_PASSWORD")
        if not username or not password:
            raise MatchbookAPIError(
                0, "MATCHBOOK_USER and MATCHBOOK_PASSWORD must be set in .env"
            )

        session = await self._ensure_session()
        url = f"{config.API_BASE_BPAPI}/security/session"
        payload = {"username": username, "password": password}

        try:
            safe_payload = {k: ("***" if k == "password" else v) for k, v in payload.items()}
            db.insert_api_log("request", "POST", url, request_body=json.dumps(safe_payload))
            async with session.post(
                url, json=payload, headers=self._auth_headers()
            ) as resp:
                body = await resp.text()
                await self._rate_limit()
                db.insert_api_log("response", "POST", url, resp.status, response_body=body[:5000])

                if resp.status == 200:
                    data = json.loads(body) if body else {}
                    self._session_token = data.get("session-token")
                    self._account = data.get("account", {})
                    if not self._session_token:
                        raise MatchbookAPIError(200, "No session-token in response", body)
                    # If login account is empty or has no balance, fetch from dedicated balance endpoint
                    has_balance = (self._account.get("balance") or 0) != 0 or (self._account.get("free-funds") or 0) != 0
                    if not self._account or not has_balance:
                        try:
                            bal_status, bal_body = await self._request_with_retry(
                                "GET", f"{config.API_BASE_EDGE}/account/balance", retry_on_401=False
                            )
                            if bal_status == 200 and bal_body:
                                bal_data = json.loads(bal_body)
                                self._account = {
                                    "balance": bal_data.get("balance", 0),
                                    "exposure": bal_data.get("exposure", 0),
                                    "free-funds": bal_data.get("free-funds", 0),
                                }
                        except Exception as e:
                            logger.warning("Could not fetch balance from /account/balance: %s", e)
                    self._save_session()
                    logger.info("Login successful (session persisted)")
                    return data
                else:
                    await self._check_suspended(resp.status, body)
                    err_msg = "Login failed"
                    if resp.status == 429:
                        retry_after = resp.headers.get("Retry-After")
                        if retry_after:
                            err_msg = f"Login rate-limited (429). Retry-After: {retry_after}s"
                        else:
                            err_msg = "Login rate-limited (429). Too many auth attempts."
                    try:
                        err_data = json.loads(body)
                        msgs = err_data.get("errors", [{}])[0].get("messages", [])
                        if msgs:
                            err_msg = msgs[0]
                    except (json.JSONDecodeError, IndexError, KeyError):
                        pass
                    raise MatchbookAPIError(resp.status, err_msg, body)
        except aiohttp.ClientError as e:
            logger.error("Network error during login: %s", e)
            db.insert_api_log("response", "POST", url, error=str(e))
            alerts.send_alert(f"Login failed (network): {e}", "auth_failure")
            raise
        except asyncio.TimeoutError:
            logger.error("Login timeout")
            db.insert_api_log("response", "POST", url, error="Timeout")
            alerts.send_alert("Login failed: timeout", "auth_failure")
            raise
        except MatchbookAPIError as e:
            db.insert_api_log("response", "POST", url, getattr(e, 'status', 0), error=str(e))
            alerts.send_alert(f"Login failed: {e}", "auth_failure")
            raise

    async def ensure_auth(self) -> None:
        """Ensure we have a session. Only login if token missing (session expired)."""
        if not self._session_token:
            await self.login()

    async def _request_with_retry(
        self, method: str, url: str, retry_on_401: bool = True, **kwargs
    ) -> tuple[int, str]:
        """
        Make HTTP request. On 401, clear session, login, retry once.
        Returns (status_code, body).
        """
        custom_headers = kwargs.pop("headers", None)
        req_headers = custom_headers or self._auth_headers()
        req_body = json.dumps(kwargs.get("json")) if kwargs.get("json") else (str(kwargs.get("params")) if kwargs.get("params") else None)
        db.insert_api_log("request", method, url, request_body=req_body)
        session = await self._ensure_session()
        async with session.request(
            method, url, headers=req_headers, **kwargs
        ) as resp:
            body = await resp.text()
            await self._rate_limit()
            db.insert_api_log("response", method, url, resp.status, response_body=body[:5000])
            if resp.status == 401 and retry_on_401 and self._session_token:
                self._clear_session()
                await self.login()
                # IMPORTANT: rebuild headers with the fresh session-token.
                retry_headers = dict(custom_headers) if custom_headers else {}
                if self._session_token:
                    retry_headers["session-token"] = self._session_token
                else:
                    retry_headers.pop("session-token", None)
                if "Content-Type" not in retry_headers:
                    retry_headers["Content-Type"] = "application/json"
                if "Accept" not in retry_headers:
                    retry_headers["Accept"] = "application/json"
                if "Accept-Encoding" not in retry_headers:
                    retry_headers["Accept-Encoding"] = "gzip"
                db.insert_api_log("request", method, url, request_body="(retry after 401)")
                async with session.request(
                    method, url, headers=retry_headers, **kwargs
                ) as retry_resp:
                    retry_body = await retry_resp.text()
                    await self._rate_limit()
                    db.insert_api_log("response", method, url, retry_resp.status, response_body=retry_body[:5000])
                    return retry_resp.status, retry_body
            return resp.status, body

    def get_account(self) -> dict:
        """
        Return account info from last login: balance, exposure, free-funds.
        Call login() or ensure_auth() first to populate.
        Handles various API key names (balance, account-balance, free-funds, free_funds).
        """
        if self._account is None:
            return {"balance": 0, "exposure": 0, "free-funds": 0}
        acc = self._account
        balance = acc.get("balance") or acc.get("account-balance")
        free_funds = acc.get("free-funds") or acc.get("free_funds")
        return {
            "balance": float(balance) if balance is not None else 0,
            "exposure": float(acc.get("exposure") or 0) if acc.get("exposure") is not None else 0,
            "free-funds": float(free_funds) if free_funds is not None else 0,
        }

    async def get_sports(self, per_page: int = 200) -> list[dict]:
        """
        Fetch list of sports from Matchbook.
        GET edge/rest/lookups/sports - use to discover correct sport-ids for get_events.
        Fetches all pages to return the full list (Football/Soccer may be beyond first 20).
        """
        await self.ensure_auth()
        url = f"{config.API_BASE_EDGE}/lookups/sports"
        all_sports: list[dict] = []
        offset = 0
        try:
            while True:
                params = {"per-page": per_page, "offset": offset}
                status, body = await self._request_with_retry(
                    "GET", url, params=params, retry_on_401=False
                )
                if status != 200:
                    raise MatchbookAPIError(status, f"get_sports failed: {body[:200]}", body)
                data = json.loads(body) if body else {}
                sports = data.get("sports", [])
                if not sports:
                    break
                all_sports.extend(sports)
                total = data.get("total", 0)
                if offset + len(sports) >= total:
                    break
                offset += len(sports)
            return all_sports
        except aiohttp.ClientError as e:
            logger.error("get_sports network error: %s", e)
            raise
        except asyncio.TimeoutError:
            logger.error("get_sports timeout")
            raise

    async def get_events(
        self,
        sport_ids: Optional[list[int]] = None,
        event_ids: Optional[list[int]] = None,
        include_prices: bool = True,
        price_depth: int = 3,
        states: str = "open",
        per_page: int = 20,
        offset: int = 0,
        pre_match_only: Optional[bool] = None,
    ) -> list[dict]:
        """
        Fetch events with optional prices.
        GET edge/rest/events.
        event_ids: fetch specific events by ID (skips pre_match filter).
        pre_match_only: when True, only return events starting in the future (excludes in-play).
        """
        await self.ensure_auth()
        url = f"{config.API_BASE_EDGE}/events"
        params = {
            "include-prices": str(include_prices).lower(),
            "price-depth": price_depth,
            "states": states,
            "exchange-type": "back-lay",
            "odds-type": "DECIMAL",
            "per-page": per_page,
            "offset": offset,
            "minimum-liquidity": 0,  # Include all prices (default 2 can filter out thin markets)
            "markets-limit": 20,  # Max markets per event (ensure we get match odds, O/U, etc.)
            "_": int(time.time() * 1000),  # Cache-bust: avoid stale cached responses
        }
        if sport_ids:
            params["sport-ids"] = ",".join(str(s) for s in sport_ids)
        if event_ids:
            params["ids"] = ",".join(str(e) for e in event_ids)
        # Only apply pre_match filter when not fetching by specific ids
        if not event_ids and (pre_match_only if pre_match_only is not None else db.get_pre_match_only()):
            params["after"] = int(time.time())

        headers = {**self._auth_headers(), "Cache-Control": "no-cache", "Pragma": "no-cache"}
        try:
            status, body = await self._request_with_retry("GET", url, params=params, headers=headers)
            await self._check_suspended(status, body)
            if status != 200:
                raise MatchbookAPIError(status, f"get_events failed: {body[:200]}", body)
            data = json.loads(body) if body else {}
            return data.get("events", [])
        except aiohttp.ClientError as e:
            logger.error("get_events network error: %s", e)
            raise
        except asyncio.TimeoutError:
            logger.error("get_events timeout")
            raise

    @staticmethod
    def passes_liquidity_filter(
        event: dict,
        market: dict,
        min_event_volume: float,
        min_market_volume: float,
        allowed_category_ids: Optional[list[int]] = None,
    ) -> bool:
        """
        Check if event/market meets liquidity thresholds.
        Event and market have a 'volume' field (total matched, in account currency).
        """
        ev_vol = float(event.get("volume", 0) or 0)
        mkt_vol = float(market.get("volume", 0) or 0)
        if ev_vol < min_event_volume or mkt_vol < min_market_volume:
            return False
        if not allowed_category_ids:
            return True
        cats = event.get("category-id") or []
        if isinstance(cats, (int, float)):
            cats = [cats]
        event_cats = {int(c) for c in cats if c is not None}
        return bool(event_cats & set(allowed_category_ids))

    async def submit_offers(self, offers: list[dict]) -> list[dict]:
        """
        Submit one or more offers (Back or Lay orders).
        POST edge/rest/v2/offers.
        In paper trading mode: no-op, logs and returns empty list.
        """
        if db.get_paper_trading():
            logger.info("PAPER TRADING: Would submit %d offers", len(offers))
            for o in offers:
                logger.info("  PAPER: %s %s @ %.2f x %.2f", o.get("side"), o.get("runner-id"), o.get("odds"), o.get("stake"))
            db.insert_api_log(
                "request",
                "POST",
                f"{config.API_BASE_EDGE}/v2/offers",
                request_body=f"[PAPER] Would submit {len(offers)} offer(s): " + str(offers)[:2000],
            )
            db.insert_api_log(
                "response",
                "POST",
                f"{config.API_BASE_EDGE}/v2/offers",
                200,
                response_body="[PAPER] Simulated - no real order sent",
            )
            return []

        await self.ensure_auth()
        url = f"{config.API_BASE_EDGE}/v2/offers"
        payload = {
            "odds-type": "DECIMAL",
            "exchange-type": "back-lay",
            "offers": offers,
        }

        try:
            status, body = await self._request_with_retry("POST", url, json=payload)
            await self._check_suspended(status, body)
            if status != 200:
                err_msg = body[:500] if body else "Unknown error"
                raise MatchbookAPIError(status, f"submit_offers failed: {err_msg}", body)
            data = json.loads(body) if body else {}
            return data.get("offers", [])
        except aiohttp.ClientError as e:
            logger.error("submit_offers network error: %s", e)
            raise
        except asyncio.TimeoutError:
            logger.error("submit_offers timeout")
            raise

    async def cancel_offers(
        self,
        offer_ids: Optional[list[int]] = None,
        market_ids: Optional[list[int]] = None,
        event_ids: Optional[list[int]] = None,
    ) -> list[dict]:
        """
        Cancel open offers.
        DELETE edge/rest/v2/offers with optional filters.
        In paper trading mode: no-op, logs and returns empty list.
        """
        if db.get_paper_trading():
            logger.info("PAPER TRADING: Would cancel offers (ids=%s)", offer_ids)
            return []

        await self.ensure_auth()
        url = f"{config.API_BASE_EDGE}/v2/offers"
        params = {}
        if offer_ids:
            params["offer-ids"] = ",".join(str(o) for o in offer_ids)
        if market_ids:
            params["market-ids"] = ",".join(str(m) for m in market_ids)
        if event_ids:
            params["event-ids"] = ",".join(str(e) for e in event_ids)

        try:
            status, body = await self._request_with_retry("DELETE", url, params=params)
            await self._check_suspended(status, body)
            if status not in (200, 204):
                raise MatchbookAPIError(
                    status, f"cancel_offers failed: {body[:200]}", body
                )
            if body:
                data = json.loads(body)
                return data.get("offers", [])
            return []
        except aiohttp.ClientError as e:
            logger.error("cancel_offers network error: %s", e)
            raise
        except asyncio.TimeoutError:
            logger.error("cancel_offers timeout")
            raise

    async def get_offers(
        self,
        offer_ids: Optional[list[int]] = None,
        statuses: Optional[list[str]] = None,
    ) -> list[dict]:
        """
        Fetch offers (open, matched, etc).
        GET edge/rest/v2/offers.
        """
        await self.ensure_auth()
        url = f"{config.API_BASE_EDGE}/v2/offers"
        params = {}
        if offer_ids:
            params["offer-ids"] = ",".join(str(o) for o in offer_ids)
        if statuses:
            params["statuses"] = ",".join(statuses)

        try:
            status, body = await self._request_with_retry("GET", url, params=params)
            await self._check_suspended(status, body)
            if status != 200:
                raise MatchbookAPIError(status, f"get_offers failed: {body[:200]}", body)
            data = json.loads(body) if body else {}
            return data.get("offers", [])
        except aiohttp.ClientError as e:
            logger.error("get_offers network error: %s", e)
            raise
        except asyncio.TimeoutError:
            logger.error("get_offers timeout")
            raise

    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
