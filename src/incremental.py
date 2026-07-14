"""
Incremental load (for scheduled runs after the backfill is done).

Two parts each run:
  1. CREATED orders  - from the newest time_stamp already in the table
     (minus a small overlap, to catch any stragglers) up to *now*.
  2. UPDATED orders  - a rolling look-back window (default 45 days) for status
     changes (chargebacks, refunds, voids, ...), because those happen to orders
     that were created long ago.

Everything MERGEs on order_id, so re-pulling the overlap is harmless (idempotent).
"""
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .bq_loader import BQLoader
from .config import AppConfig, Settings
from .pipeline import run_window
from .sticky_client import StickyAPIClient

logger = logging.getLogger(__name__)

_TS = "%Y-%m-%d %H:%M:%S"


async def run_incremental(
    client: StickyAPIClient, loader: BQLoader, app: AppConfig, settings: Settings,
) -> None:
    now_local = datetime.now(ZoneInfo(app.source_timezone))
    end_date = now_local.strftime("%m/%d/%Y")
    end_time = now_local.strftime("%H:%M:%S")

    # ---- 1) created orders since watermark ----
    watermark = loader.get_watermark(app.name, source="original")
    if watermark:
        start_dt = datetime.strptime(watermark, _TS) - timedelta(
            days=settings.incremental_original_overlap_days)
    else:
        # No data yet -> fall back to the backfill start date.
        start_dt = datetime.combine(settings.backfill_start, datetime.min.time())
    sd = start_dt.strftime("%m/%d/%Y")
    st = start_dt.strftime("%H:%M:%S")
    logger.info(f"[{app.company}] INCREMENTAL created from {sd} {st} -> {end_date} {end_time}",
                extra={"company": app.company, "mode": "incremental"})
    loaded, max_ts = await run_window(
        client, loader, app, settings, start_date=sd, end_date=end_date,
        source="original", start_time=st, end_time=end_time)
    loader.record_state(app.company, "incremental", "created", "original", loaded, max_ts)

    # ---- 2) updated orders: rolling look-back ----
    lookback_start = (now_local - timedelta(days=settings.incremental_updated_lookback_days))
    sd_u = lookback_start.strftime("%m/%d/%Y")
    logger.info(f"[{app.company}] INCREMENTAL updated {sd_u} -> {end_date} "
                f"(last {settings.incremental_updated_lookback_days}d)",
                extra={"company": app.company, "mode": "incremental"})
    loaded_u, max_ts_u = await run_window(
        client, loader, app, settings, start_date=sd_u, end_date=end_date,
        source="updated", end_time=end_time)
    loader.record_state(app.company, "incremental", "updated", "updated", loaded_u, max_ts_u)

    logger.info(f"[{app.company}] INCREMENTAL complete",
                extra={"company": app.company, "mode": "incremental"})
