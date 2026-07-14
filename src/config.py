"""
Configuration for the Sticky.io -> BigQuery ETL.

Everything that changes between environments or runs comes from environment
variables (set on the Cloud Run Job) plus a small YAML registry of apps
(config/apps.yaml). NO secrets and NO credentials are ever stored in code.

The sticky.io username/password for each app lives in Google Secret Manager.
The Cloud Run Job authenticates to BigQuery and Secret Manager automatically
using its own service-account identity (Application Default Credentials) -
there is no service-account key file anywhere.
"""
import logging
import os
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


@dataclass
class AppConfig:
    """Settings for a single sticky.io application (one merchant account)."""
    name: str                      # logical name, used in the BigQuery table name
    company: str                   # sticky.io subdomain, e.g. "pdfdotnet"
    cred_secret: str               # Secret Manager secret name holding "user:pass"
    source_timezone: str = "America/New_York"   # timezone sticky reports times in
    report_timezone: str = "America/New_York"   # timezone used for date_of_sale
    campaign_id: str = "all"


@dataclass
class Settings:
    """Global run settings assembled from environment variables."""
    mode: str                      # "backfill" or "incremental"
    bq_project: str
    bq_dataset: str
    bq_location: str               # e.g. "US", "EU", "europe-west1"
    apps: List[AppConfig]
    selected_company: str          # which app to run, or "all"

    backfill_start: date           # first day to back-fill from (default 2025-11-01)
    incremental_original_overlap_days: int = 1   # re-pull recent created orders
    incremental_updated_lookback_days: int = 45  # window for status-change pulls

    rate_limit_per_minute: int = 15
    concurrency: int = 80
    order_view_batch_size: int = 500
    bq_flush_rows: int = 5000      # rows buffered before a load into staging
    request_timeout_seconds: int = 3600
    log_level: str = "INFO"

    # group_keys for order_find_updated (status changes we care about)
    updated_group_keys: List[str] = field(default_factory=lambda: [
        "chargeback", "confirmation", "fraud", "refund",
        "reprocess", "return", "rma", "void",
    ])


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Required environment variable '{name}' is not set. "
            f"Set it on the Cloud Run Job (see README)."
        )
    return value


def _load_apps(path: str) -> List[AppConfig]:
    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    apps = []
    for item in raw.get("apps", []):
        apps.append(AppConfig(
            name=item["name"],
            company=item["company"],
            cred_secret=item["cred_secret"],
            source_timezone=item.get("source_timezone", "America/New_York"),
            report_timezone=item.get("report_timezone", "America/New_York"),
            campaign_id=str(item.get("campaign_id", "all")),
        ))
    if not apps:
        raise RuntimeError(f"No apps defined in {path}")
    return apps


def load_settings(apps_path: str = "config/apps.yaml") -> Settings:
    mode = os.environ.get("MODE", "incremental").strip().lower()
    if mode not in ("backfill", "incremental"):
        raise RuntimeError(f"MODE must be 'backfill' or 'incremental', got '{mode}'")

    backfill_start_str = os.environ.get("BACKFILL_START", "2025-11-01")
    year, month, day = (int(x) for x in backfill_start_str.split("-"))

    settings = Settings(
        mode=mode,
        bq_project=_require("BQ_PROJECT"),
        bq_dataset=_require("BQ_DATASET"),
        bq_location=os.environ.get("BQ_LOCATION", "US"),
        apps=_load_apps(apps_path),
        selected_company=os.environ.get("STICKY_COMPANY", "all").strip(),
        backfill_start=date(year, month, day),
        incremental_original_overlap_days=int(
            os.environ.get("INCREMENTAL_ORIGINAL_OVERLAP_DAYS", "1")),
        incremental_updated_lookback_days=int(
            os.environ.get("INCREMENTAL_UPDATED_LOOKBACK_DAYS", "45")),
        rate_limit_per_minute=int(os.environ.get("RATE_LIMIT_PER_MINUTE", "15")),
        concurrency=int(os.environ.get("CONCURRENCY", "80")),
        order_view_batch_size=int(os.environ.get("ORDER_VIEW_BATCH_SIZE", "500")),
        bq_flush_rows=int(os.environ.get("BQ_FLUSH_ROWS", "5000")),
        request_timeout_seconds=int(os.environ.get("REQUEST_TIMEOUT_SECONDS", "3600")),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
    )
    return settings


def apps_to_run(settings: Settings) -> List[AppConfig]:
    """Return the list of apps this execution should process."""
    if settings.selected_company.lower() == "all":
        return settings.apps
    for app in settings.apps:
        if app.company == settings.selected_company or app.name == settings.selected_company:
            return [app]
    raise RuntimeError(
        f"STICKY_COMPANY='{settings.selected_company}' not found in config/apps.yaml"
    )


def resolve_credential(cred_secret: str, bq_project: str) -> str:
    """
    Fetch the sticky.io "user:pass" string.

    Priority:
      1. Environment variable STICKY_CRED  (handy for local testing only)
      2. Google Secret Manager secret named by `cred_secret`
    """
    env_cred = os.environ.get("STICKY_CRED")
    if env_cred:
        logger.info("Using sticky credential from STICKY_CRED env var (local mode).")
        return env_cred.strip()

    from google.cloud import secretmanager
    client = secretmanager.SecretManagerServiceClient()
    # Support either a bare secret name or a full resource path.
    if cred_secret.startswith("projects/"):
        name = cred_secret
    else:
        name = f"projects/{bq_project}/secrets/{cred_secret}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("utf-8").strip()
