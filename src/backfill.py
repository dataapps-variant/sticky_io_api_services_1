"""
Backfill: load history month by month from BACKFILL_START up to *today*
(computed live at run time - never a hard-coded date), then it's done.

Each month is checkpointed in the _etl_state table. If the Job is interrupted
or re-run, already-completed months are skipped, so you can safely run it again
and it resumes. Splitting by month keeps each unit of work bounded and easy to
verify against the sticky.io Orders report.
"""
import logging
from datetime import date, datetime, timedelta

from .bq_loader import BQLoader
from .config import AppConfig, Settings
from .pipeline import run_window
from .sticky_client import StickyAPIClient

logger = logging.getLogger(__name__)


def _month_windows(start: date, today: date):
    """Yield (window_key, first_day, last_day) for each month from start..today."""
    y, m = start.year, start.month
    while date(y, m, 1) <= today:
        first = date(y, m, 1)
        # first day of next month
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        last = date(ny, nm, 1) - timedelta(days=1)
        if last > today:
            last = today
        yield f"{y:04d}-{m:02d}", first, last
        y, m = ny, nm


async def run_backfill(
    client: StickyAPIClient, loader: BQLoader, app: AppConfig, settings: Settings,
    include_updated: bool = True,
) -> None:
    today = datetime.now().date()  # dynamic "today"
    logger.info(f"[{app.company}] BACKFILL {settings.backfill_start} -> {today}",
                extra={"company": app.company, "mode": "backfill"})

    done_original = loader.completed_windows(app.company, "backfill", "original")
    done_updated = loader.completed_windows(app.company, "backfill", "updated")

    for window_key, first, last in _month_windows(settings.backfill_start, today):
        sd = first.strftime("%m/%d/%Y")
        ed = last.strftime("%m/%d/%Y")

        # --- created orders for the month ---
        if window_key in done_original:
            logger.info(f"[{app.company}] {window_key} original already done, skipping")
        else:
            loaded, max_ts = await run_window(
                client, loader, app, settings, sd, ed, source="original")
            loader.record_state(app.company, "backfill", window_key, "original", loaded, max_ts)

        # --- status-changed orders for the month ---
        if include_updated:
            if window_key in done_updated:
                logger.info(f"[{app.company}] {window_key} updated already done, skipping")
            else:
                loaded_u, max_ts_u = await run_window(
                    client, loader, app, settings, sd, ed, source="updated")
                loader.record_state(app.company, "backfill", window_key, "updated", loaded_u, max_ts_u)

    logger.info(f"[{app.company}] BACKFILL complete through {today}",
                extra={"company": app.company, "mode": "backfill"})
