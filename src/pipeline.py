"""
The reusable core: given a date window and a source ('original' or 'updated'),
find the order IDs, fetch the full orders, flatten them into WIDE rows (one column
per field), and append them into the app's orders table.

Loads are append-only (which is simple and safe for wide/growing schemas). Because
re-running a window can append the same order again, query the de-duplicated view
orders_<app>_latest (newest row per order_id) - see sql/orders_latest_view.sql.
The backfill checkpoints each month, so it never re-does completed months.
"""
import logging
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from .bq_loader import BQLoader
from .config import AppConfig, Settings
from .sticky_client import StickyAPIClient
from .transform import order_to_flat_row

logger = logging.getLogger(__name__)


async def run_window(
    client: StickyAPIClient,
    loader: BQLoader,
    app: AppConfig,
    settings: Settings,
    start_date: str,      # MM/DD/YYYY
    end_date: str,        # MM/DD/YYYY
    source: str,          # 'original' or 'updated'
    start_time: str = "00:00:00",
    end_time: str = "23:59:59",
) -> Tuple[int, Optional[str]]:
    """Process one window. Returns (orders_loaded, max_time_stamp_seen)."""
    logger.info(
        f"[{app.company}/{source}] window {start_date} {start_time} -> {end_date} {end_time}",
        extra={"company": app.company, "source": source,
               "window": f"{start_date}-{end_date}"},
    )

    if source == "original":
        order_ids, reported = await client.find_order_ids(
            start_date, end_date, start_time, end_time,
            campaign_id=app.campaign_id, date_type="create",
        )
    else:
        order_ids, reported = await client.find_updated_order_ids(
            start_date, end_date, start_time, end_time,
            campaign_id=app.campaign_id, group_keys=settings.updated_group_keys,
        )
    logger.info(f"[{app.company}/{source}] {len(order_ids)} order ids to fetch "
                f"(sticky reported {reported})")
    if not order_ids:
        return 0, None

    loaded_at_iso = datetime.now(timezone.utc).isoformat()
    buffer: List[dict] = []
    total_loaded = 0
    max_ts: Optional[str] = None

    async for order_group in client.stream_view_orders(order_ids):
        for order in order_group:
            row = order_to_flat_row(
                order, company=app.company, source=source,
                source_tz=app.source_timezone, report_tz=app.report_timezone,
                loaded_at_iso=loaded_at_iso,
            )
            if row is None:
                continue
            buffer.append(row)
            ts = order.get("time_stamp")
            if ts and (max_ts is None or ts > max_ts):
                max_ts = ts
        if len(buffer) >= settings.bq_flush_rows:
            loader.append_flat_rows(app.name, buffer)
            total_loaded += len(buffer)
            logger.info(f"[{app.company}/{source}] appended {total_loaded} rows")
            buffer = []

    if buffer:
        loader.append_flat_rows(app.name, buffer)
        total_loaded += len(buffer)

    logger.info(f"[{app.company}/{source}] done: {total_loaded} rows into orders_{app.name}",
                extra={"company": app.company, "source": source, "orders": total_loaded})
    return total_loaded, max_ts
