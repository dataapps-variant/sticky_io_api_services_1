"""
Sticky.io API client.

This is a cleaned-up port of the pagination and rate-limiting logic from the
original middleware (main.py). The important, hard-won behaviours are preserved:

  * order_find is paginated to get around sticky.io's 50,000-order-per-response
    cap, by walking forward in time using the time_stamp of the last order in
    each 50K page.
  * order_view is fetched in batches of 500 order IDs (sticky.io's limit),
    concurrently, behind a sliding-window rate limiter (default 15 req/min).
  * Failed requests are retried with exponential backoff.

The one deliberate difference from the original: this client does NOT drop or
flatten any fields. It returns each order exactly as sticky.io sends it, so that
nothing (utm_info / device_category, custom_fields, order_customer_types, the
full products array, etc.) is ever lost. All shaping happens later, in SQL views
on top of the raw JSON we store.
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, List, Tuple

import aiohttp

logger = logging.getLogger(__name__)

# sticky.io returns at most this many order IDs in one order_find response.
STICKY_ORDER_FIND_CAP = 50000


class StickyAPIClient:
    def __init__(
        self,
        company: str,
        username: str,
        password: str,
        rate_limit_per_minute: int = 15,
        concurrency: int = 80,
        order_view_batch_size: int = 500,
        request_timeout_seconds: int = 3600,
    ):
        self.base_url = f"https://{company}.sticky.io/api/v1/"
        self._auth = aiohttp.BasicAuth(username, password)
        self.rate_limit_per_minute = rate_limit_per_minute
        self.order_view_batch_size = order_view_batch_size
        self.request_timeout_seconds = request_timeout_seconds

        self._session: aiohttp.ClientSession | None = None
        self._semaphore = asyncio.Semaphore(concurrency)
        self._request_times: List[float] = []          # sliding window of send times
        self._rate_lock = asyncio.Lock()               # protect the sliding window

    # ------------------------------------------------------------------ session
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            auth=self._auth,
            headers={"Content-Type": "application/json"},
        )
        return self

    async def __aexit__(self, *exc):
        if self._session:
            await self._session.close()

    # --------------------------------------------------------------- rate limit
    async def _rate_limit(self) -> None:
        """Sliding-window limiter: never send more than N requests per 60s."""
        async with self._rate_lock:
            now = time.time()
            self._request_times = [t for t in self._request_times if now - t < 60]
            if len(self._request_times) >= self.rate_limit_per_minute:
                wait = 60 - (now - self._request_times[0]) + 0.1
                if wait > 0:
                    await asyncio.sleep(wait)
                    now = time.time()
                    self._request_times = [t for t in self._request_times if now - t < 60]
            self._request_times.append(time.time())

    async def _post(self, endpoint: str, payload: dict, max_retries: int = 4) -> dict:
        """POST with rate limiting and exponential-backoff retries.

        IMPORTANT: sticky.io can return HTTP 200 "OK" while the JSON body itself
        reports a real error, e.g. {"response_code":"668","error_message":
        "Unauthorized IP Address"}. If we only checked the HTTP status, this would
        be silently treated as a valid (empty) response -> orders would appear to
        be "0 found" instead of surfacing the real problem. So we explicitly check
        for an error response_code / status inside the body too.
        """
        delay = 2
        for attempt in range(1, max_retries + 1):
            await self._rate_limit()
            try:
                async with self._session.post(self.base_url + endpoint, json=payload) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        if attempt == max_retries:
                            raise RuntimeError(
                                f"{endpoint} failed after {max_retries} attempts "
                                f"(HTTP {resp.status}): {text[:500]}"
                            )
                        logger.warning(f"{endpoint} HTTP {resp.status}, retry {attempt}: {text[:200]}")
                        await asyncio.sleep(delay)
                        delay *= 2
                        continue

                    body = await resp.json()
                    # Detect sticky.io-style embedded errors even on HTTP 200.
                    resp_code = str(body.get("response_code", "")).strip()
                    status_field = str(body.get("status", "")).strip().upper()
                    err_msg = body.get("error_message")
                    looks_like_error = (
                        (resp_code and resp_code not in ("100", "success", ""))
                        or status_field == "FAILURE"
                        or err_msg
                    )
                    if looks_like_error:
                        detail = f"response_code={resp_code!r} status={status_field!r} error_message={err_msg!r}"
                        if attempt == max_retries:
                            raise RuntimeError(f"{endpoint} returned an error body (HTTP 200): {detail}")
                        logger.warning(f"{endpoint} error body, retry {attempt}: {detail}")
                        await asyncio.sleep(delay)
                        delay *= 2
                        continue

                    return body
            except aiohttp.ClientError as exc:
                if attempt == max_retries:
                    raise RuntimeError(f"{endpoint} network error after {max_retries} attempts: {exc}")
                logger.warning(f"{endpoint} network error, retry {attempt}: {exc}")
                await asyncio.sleep(delay)
                delay *= 2
        raise RuntimeError(f"{endpoint} exhausted retries")

    # ---------------------------------------------------------------- order_find
    @staticmethod
    def _timestamp_to_find_params(timestamp: str) -> Tuple[str, str]:
        """'2025-11-14 09:32:07' -> ('11/14/2025', '09:32:07') for order_find."""
        dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%m/%d/%Y"), dt.strftime("%H:%M:%S")

    async def find_order_ids(
        self,
        start_date: str,      # MM/DD/YYYY
        end_date: str,        # MM/DD/YYYY
        start_time: str = "00:00:00",
        end_time: str = "23:59:59",
        campaign_id: str = "all",
        date_type: str = "create",
    ) -> Tuple[List[str], int]:
        """
        Return (unique_order_ids, total_reported) for *created* orders in a window,
        paginating past the 50K cap using each page's last order's time_stamp.
        """
        all_ids: List[str] = []
        cur_start_date, cur_start_time = start_date, start_time
        total_reported = 0
        chunk = 0

        while True:
            chunk += 1
            payload = {
                "campaign_id": campaign_id,
                "start_date": cur_start_date,
                "end_date": end_date,
                "start_time": cur_start_time,
                "end_time": end_time,
                "date_type": date_type,
                "criteria": "all",
                "search_type": "all",
            }
            resp = await self._post("order_find", payload)
            ids = resp.get("order_id", []) or []
            chunk_total = int(resp.get("total_orders", "0") or 0)
            if chunk == 1:
                total_reported = chunk_total
            logger.info(f"order_find chunk {chunk}: {len(ids)} ids (window total {chunk_total})")
            all_ids.extend(str(i) for i in ids)

            if len(ids) < STICKY_ORDER_FIND_CAP:
                break
            # Hit the cap -> advance the window start to the last order's timestamp.
            last_id = str(ids[-1])
            details = await self.view_orders([last_id])
            ts = details[0].get("time_stamp") if details else None
            if not ts:
                logger.error(f"No time_stamp for last order {last_id}; stopping pagination.")
                break
            cur_start_date, cur_start_time = self._timestamp_to_find_params(ts)

        unique = list(dict.fromkeys(all_ids))
        removed = len(all_ids) - len(unique)
        if removed:
            logger.info(f"order_find: removed {removed} duplicate ids across chunks")
        return unique, total_reported

    async def find_updated_order_ids(
        self,
        start_date: str,
        end_date: str,
        start_time: str = "00:00:00",
        end_time: str = "23:59:59",
        campaign_id: str = "all",
        group_keys: List[str] | None = None,
    ) -> Tuple[List[str], int]:
        """Return (unique_order_ids, total) for orders whose STATUS changed."""
        group_keys = group_keys or [
            "chargeback", "confirmation", "fraud", "refund",
            "reprocess", "return", "rma", "void",
        ]
        all_ids: List[str] = []
        cur_start_date, cur_start_time = start_date, start_time
        total_reported = 0
        chunk = 0

        while True:
            chunk += 1
            payload = {
                "campaign_id": campaign_id,
                "start_date": cur_start_date,
                "end_date": end_date,
                "start_time": cur_start_time,
                "end_time": end_time,
                "group_keys": group_keys,
            }
            resp = await self._post("order_find_updated", payload)
            ids = resp.get("order_id", []) or []
            chunk_total = int(resp.get("total_orders", "0") or 0)
            if chunk == 1:
                total_reported = chunk_total
            logger.info(f"order_find_updated chunk {chunk}: {len(ids)} ids")
            all_ids.extend(str(i) for i in ids)

            if len(ids) < STICKY_ORDER_FIND_CAP:
                break
            last_id = str(ids[-1])
            details = await self.view_orders([last_id])
            ts = details[0].get("time_stamp") if details else None
            if not ts:
                break
            cur_start_date, cur_start_time = self._timestamp_to_find_params(ts)

        unique = list(dict.fromkeys(all_ids))
        return unique, total_reported

    # ---------------------------------------------------------------- order_view
    async def _view_batch(self, batch: List[str], batch_num: int) -> List[Dict]:
        async with self._semaphore:
            payload = {"order_id": [int(o) for o in batch], "return_variants": 1}
            resp = await self._post("order_view", payload)
            data = resp.get("data")
            if isinstance(data, dict):
                orders = list(data.values())
            elif isinstance(data, list):
                orders = data
            else:
                orders = [resp]
            logger.info(f"order_view batch {batch_num}: {len(orders)} orders")
            return orders

    async def view_orders(self, order_ids: List[str]) -> List[Dict]:
        """Fetch full order objects for the given IDs, in concurrent batches of 500.

        Returns the orders UNMODIFIED (nothing dropped or flattened)."""
        if not order_ids:
            return []
        size = self.order_view_batch_size
        batches = [order_ids[i:i + size] for i in range(0, len(order_ids), size)]
        tasks = [asyncio.create_task(self._view_batch(b, n + 1)) for n, b in enumerate(batches)]
        results: List[Dict] = []
        done = 0
        for coro in asyncio.as_completed(tasks):
            results.extend(await coro)
            done += 1
            if done % 10 == 0 or done == len(batches):
                logger.info(f"order_view progress: {done}/{len(batches)} batches")
        return results

    async def stream_view_orders(self, order_ids: List[str], batch_group: int = 10):
        """
        Yield orders in groups so the caller can flush to BigQuery incrementally
        instead of holding an entire month in memory. Yields lists of order dicts.
        """
        size = self.order_view_batch_size
        id_batches = [order_ids[i:i + size] for i in range(0, len(order_ids), size)]
        for g in range(0, len(id_batches), batch_group):
            group = id_batches[g:g + batch_group]
            tasks = [
                asyncio.create_task(self._view_batch(b, g + n + 1))
                for n, b in enumerate(group)
            ]
            collected: List[Dict] = []
            for coro in asyncio.as_completed(tasks):
                collected.extend(await coro)
            yield collected
