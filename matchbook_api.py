"""
Asynchronous Matchbook Exchange API wrapper.
Handles authentication (session tokens, expiry), market data, and order execution.
Loads credentials from .env via python-dotenv.
"""

import asyncio
import json
import logging
import os
from typing import Any, Optional

import aiohttp
from dotenv import load_dotenv

import config

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
            async with session.post(
                url, json=payload, headers=self._auth_headers()
            ) as resp:
                body = await resp.text()
                await self._rate_limit()

                if resp.status == 200:
                    data = json.loads(body) if body else {}
                    self._session_token = data.get("session-token")
                    # Account info: balance, exposure, free-funds
                    self._account = data.get("account", {})
                    if not self._session_token:
                        raise MatchbookAPIError(200, "No session-token in response", body)
                    logger.info("Login successful")
                    return data
                else:
                    await self._check_suspended(resp.status, body)
                    err_msg = "Login failed"
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
            raise
        except asyncio.TimeoutError:
            logger.error("Login timeout")
            raise

    async def get_session(self) -> bool:
        """
        Validate session token. GET bpapi/rest/security/session.
        Returns True if valid, False if expired (401).
        """
        if not self._session_token:
            return False
        session = await self._ensure_session()
        url = f"{config.API_BASE_BPAPI}/security/session"
        try:
            async with session.get(url, headers=self._auth_headers()) as resp:
                await self._rate_limit()
                if resp.status == 200:
                    return True
                if resp.status == 401:
                    self._session_token = None
                    self._account = None
                    return False
                return False
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return False

    async def ensure_auth(self) -> None:
        """Ensure we have a valid session; re-login if expired."""
        if not self._session_token or not await self.get_session():
            await self.login()

    def get_account(self) -> dict:
        """
        Return account info from last login: balance, exposure, free-funds.
        Call login() or ensure_auth() first to populate.
        """
        if self._account is None:
            return {"balance": 0, "exposure": 0, "free-funds": 0}
        return self._account

    async def get_events(
        self,
        sport_ids: Optional[list[int]] = None,
        include_prices: bool = True,
        price_depth: int = 3,
        states: str = "open",
        per_page: int = 20,
        offset: int = 0,
    ) -> list[dict]:
        """
        Fetch events with optional prices.
        GET edge/rest/events.
        Filter by sport-ids for football (1) or political category-ids.
        """
        await self.ensure_auth()
        session = await self._ensure_session()
        url = f"{config.API_BASE_EDGE}/events"
        params = {
            "include-prices": str(include_prices).lower(),
            "price-depth": price_depth,
            "states": states,
            "exchange-type": "back-lay",
            "odds-type": "DECIMAL",
            "per-page": per_page,
            "offset": offset,
        }
        if sport_ids:
            params["sport-ids"] = ",".join(str(s) for s in sport_ids)

        try:
            async with session.get(url, params=params, headers=self._auth_headers()) as resp:
                body = await resp.text()
                await self._rate_limit()
                await self._check_suspended(resp.status, body)

                if resp.status != 200:
                    raise MatchbookAPIError(resp.status, f"get_events failed: {body[:200]}", body)

                data = json.loads(body) if body else {}
                return data.get("events", [])
        except aiohttp.ClientError as e:
            logger.error("get_events network error: %s", e)
            raise
        except asyncio.TimeoutError:
            logger.error("get_events timeout")
            raise

    async def submit_offers(self, offers: list[dict]) -> list[dict]:
        """
        Submit one or more offers (Back or Lay orders).
        POST edge/rest/v2/offers.
        Each offer: runner-id, side, odds, stake, keep-in-play (optional).
        Returns list of offer objects from response.
        """
        await self.ensure_auth()
        session = await self._ensure_session()
        url = f"{config.API_BASE_EDGE}/v2/offers"
        payload = {
            "odds-type": "DECIMAL",
            "exchange-type": "back-lay",
            "offers": offers,
        }

        try:
            async with session.post(url, json=payload, headers=self._auth_headers()) as resp:
                body = await resp.text()
                await self._rate_limit()
                await self._check_suspended(resp.status, body)

                if resp.status != 200:
                    err_msg = body[:500] if body else "Unknown error"
                    raise MatchbookAPIError(resp.status, f"submit_offers failed: {err_msg}", body)

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
        If no filters, cancels all open offers.
        """
        await self.ensure_auth()
        session = await self._ensure_session()
        url = f"{config.API_BASE_EDGE}/v2/offers"
        params = {}
        if offer_ids:
            params["offer-ids"] = ",".join(str(o) for o in offer_ids)
        if market_ids:
            params["market-ids"] = ",".join(str(m) for m in market_ids)
        if event_ids:
            params["event-ids"] = ",".join(str(e) for e in event_ids)

        try:
            async with session.delete(url, params=params, headers=self._auth_headers()) as resp:
                body = await resp.text()
                await self._rate_limit()
                await self._check_suspended(resp.status, body)

                if resp.status not in (200, 204):
                    raise MatchbookAPIError(
                        resp.status, f"cancel_offers failed: {body[:200]}", body
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
        session = await self._ensure_session()
        url = f"{config.API_BASE_EDGE}/v2/offers"
        params = {}
        if offer_ids:
            params["offer-ids"] = ",".join(str(o) for o in offer_ids)
        if statuses:
            params["statuses"] = ",".join(statuses)

        try:
            async with session.get(url, params=params, headers=self._auth_headers()) as resp:
                body = await resp.text()
                await self._rate_limit()
                await self._check_suspended(resp.status, body)

                if resp.status != 200:
                    raise MatchbookAPIError(resp.status, f"get_offers failed: {body[:200]}", body)

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
