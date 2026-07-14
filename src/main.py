"""
Entrypoint for the Cloud Run Job.

Reads its behaviour from environment variables (MODE, STICKY_COMPANY, ...),
runs either the backfill or the incremental load for the selected app(s), and
exits 0 on success / non-zero on failure so Cloud Run marks the execution
correctly.

Run locally:
    MODE=backfill STICKY_COMPANY=pdfdotnet BQ_PROJECT=... BQ_DATASET=Sticky_ETL \
    STICKY_CRED='user:pass' python -m src.main
"""
import asyncio
import logging
import sys

from .backfill import run_backfill
from .bq_loader import BQLoader
from .config import apps_to_run, load_settings, resolve_credential
from .incremental import run_incremental
from .logging_setup import setup_logging
from .sticky_client import StickyAPIClient


async def _run_app(app, settings, loader) -> None:
    logger = logging.getLogger(__name__)
    cred = resolve_credential(app.cred_secret, settings.bq_project)
    if ":" not in cred:
        raise RuntimeError(f"Credential for {app.company} must be 'user:pass'.")
    username, password = cred.split(":", 1)

    loader.ensure_dataset()
    loader.ensure_state_table()
    loader.ensure_orders_table(app.name)

    async with StickyAPIClient(
        company=app.company, username=username, password=password,
        rate_limit_per_minute=settings.rate_limit_per_minute,
        concurrency=settings.concurrency,
        order_view_batch_size=settings.order_view_batch_size,
        request_timeout_seconds=settings.request_timeout_seconds,
    ) as client:
        if settings.mode == "backfill":
            await run_backfill(client, loader, app, settings)
        else:
            await run_incremental(client, loader, app, settings)


async def _main() -> int:
    settings = load_settings()
    setup_logging(settings.log_level)
    logger = logging.getLogger(__name__)
    loader = BQLoader(settings.bq_project, settings.bq_dataset, settings.bq_location)

    targets = apps_to_run(settings)
    logger.info(f"MODE={settings.mode} apps={[a.company for a in targets]}")

    failures = []
    for app in targets:
        try:
            await _run_app(app, settings, loader)
        except Exception as exc:  # keep going with other apps, but fail the job
            logger.exception(f"[{app.company}] failed: {exc}")
            failures.append(app.company)

    if failures:
        logger.error(f"Completed with failures: {failures}")
        return 1
    logger.info("All apps completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
