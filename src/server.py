"""
Web-server front-end so this ETL runs as a Cloud Run *Service*.

Trigger runs by opening a URL (or via Cloud Scheduler).

Endpoints
---------
GET  /health
        Liveness probe -> {"status":"ok"}.

GET|POST /run?mode=<backfill|incremental>&company=<app|all>&token=<t>
        Runs the ETL. `company` may be:
          * a single app  (e.g. company=ct)  -> just that app
          * all           (company=all, or omit to use STICKY_COMPANY)
                          -> loops over EVERY app in config/apps.yaml, one after
                             another, each into its own orders_<app> table.

        mode=incremental : for each app, load new created orders since its
                           watermark + a rolling window of status changes. This
                           is what you schedule daily (company=all does them all
                           in one trigger).
        mode=backfill    : for each app, process that app's NEXT unfinished month
                           and report how many months remain. Trigger again to do
                           the next month(s).

Security: every /run call needs ?token=<RUN_TOKEN> (an env var / secret on the
service). If one app fails (e.g. a missing credential) the others still run, and
the response lists each app's result.
"""
import logging
import os
from datetime import datetime
from functools import lru_cache
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query

from .backfill import _month_windows
from .bq_loader import BQLoader
from .config import AppConfig, Settings, load_settings, resolve_credential
from .incremental import run_incremental
from .logging_setup import setup_logging
from .pipeline import run_window
from .sticky_client import StickyAPIClient

setup_logging(os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

app = FastAPI(title="Sticky.io -> BigQuery ETL (Service)", version="2.0.0")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    # Lazy so a missing env var surfaces on /run, not at container startup.
    return load_settings()


def _apps_to_run(company: Optional[str]) -> List[AppConfig]:
    settings = get_settings()
    target = (company or settings.selected_company or "all").strip()
    if target.lower() == "all":
        return settings.apps
    for a in settings.apps:
        if a.company == target or a.name == target:
            return [a]
    raise HTTPException(status_code=404, detail=f"App '{target}' not in config/apps.yaml")


def _check_token(token: Optional[str]) -> None:
    expected = os.environ.get("RUN_TOKEN")
    if not expected:
        raise HTTPException(status_code=500, detail="RUN_TOKEN is not configured on the service.")
    if token != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing token.")


async def _make_client(app_cfg: AppConfig, settings: Settings) -> StickyAPIClient:
    cred = resolve_credential(app_cfg.cred_secret, settings.bq_project)
    if ":" not in cred:
        raise RuntimeError("Credential must be in 'user:pass' form.")
    username, password = cred.split(":", 1)
    return StickyAPIClient(
        company=app_cfg.company, username=username, password=password,
        rate_limit_per_minute=settings.rate_limit_per_minute,
        concurrency=settings.concurrency,
        order_view_batch_size=settings.order_view_batch_size,
        request_timeout_seconds=settings.request_timeout_seconds,
    )


async def _run_incremental_for(app_cfg: AppConfig, settings: Settings, loader: BQLoader) -> dict:
    loader.ensure_orders_table(app_cfg.name)
    client = await _make_client(app_cfg, settings)
    async with client:
        await run_incremental(client, loader, app_cfg, settings)
    return {"company": app_cfg.company, "status": "ok", "mode": "incremental"}


async def _run_backfill_month_for(app_cfg: AppConfig, settings: Settings, loader: BQLoader) -> dict:
    loader.ensure_orders_table(app_cfg.name)
    today = datetime.now().date()
    windows = list(_month_windows(settings.backfill_start, today))
    done = loader.completed_windows(app_cfg.company, "backfill", "original")

    nxt = next(((w, f, l) for (w, f, l) in windows if w not in done), None)
    if nxt is None:
        return {"company": app_cfg.company, "status": "backfill_complete",
                "months_total": len(windows)}

    wkey, first, last = nxt
    sd, ed = first.strftime("%m/%d/%Y"), last.strftime("%m/%d/%Y")
    client = await _make_client(app_cfg, settings)
    async with client:
        loaded_o, max_o = await run_window(client, loader, app_cfg, settings, sd, ed, "original")
        loader.record_state(app_cfg.company, "backfill", wkey, "original", loaded_o, max_o)
        loaded_u, max_u = await run_window(client, loader, app_cfg, settings, sd, ed, "updated")
        loader.record_state(app_cfg.company, "backfill", wkey, "updated", loaded_u, max_u)

    remaining = sum(1 for (w, _, _) in windows if w not in done and w != wkey)
    return {"company": app_cfg.company, "status": "month_done", "month": wkey,
            "created_loaded": loaded_o, "updated_loaded": loaded_u,
            "months_remaining": remaining}


@app.get("/")
@app.get("/health")
def health():
    return {"status": "ok"}


@app.api_route("/run", methods=["GET", "POST"])
async def run(
    mode: str = Query(..., description="backfill or incremental"),
    company: Optional[str] = Query(None, description="app name/subdomain, or 'all'"),
    token: Optional[str] = Query(None, description="RUN_TOKEN"),
):
    _check_token(token)
    mode = mode.strip().lower()
    if mode not in ("backfill", "incremental"):
        raise HTTPException(status_code=400, detail="mode must be 'backfill' or 'incremental'.")

    settings = get_settings()
    targets = _apps_to_run(company)
    loader = BQLoader(settings.bq_project, settings.bq_dataset, settings.bq_location)
    loader.ensure_dataset()
    loader.ensure_state_table()

    results = []
    for app_cfg in targets:
        try:
            if mode == "incremental":
                results.append(await _run_incremental_for(app_cfg, settings, loader))
            else:
                results.append(await _run_backfill_month_for(app_cfg, settings, loader))
        except Exception as exc:
            logger.exception(f"[{app_cfg.company}] failed: {exc}")
            results.append({"company": app_cfg.company, "status": "error", "error": str(exc)})

    overall = "ok" if all(r.get("status") != "error" for r in results) else "partial_failure"
    backfill_all_done = (
        mode == "backfill"
        and all(r.get("status") in ("backfill_complete", "error") for r in results)
    )
    return {
        "mode": mode,
        "apps_run": [a.company for a in targets],
        "overall": overall,
        "results": results,
        "hint": (
            "All apps' backfill complete - switch to mode=incremental."
            if backfill_all_done else
            ("Trigger again to advance the next backfill month(s)."
             if mode == "backfill" else "Incremental pass complete.")
        ),
    }
