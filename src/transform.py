"""
Turn a raw sticky.io order into a WIDE BigQuery row - one real column per field.

Every field sticky.io returns becomes its own column:
  * top-level fields (billing_*, shipping_*, afid, order_total, ...) keep their names
  * the first product is flattened to prd_*  (and the full list kept as products_json)
  * totals_breakdown is flattened to breakdown_*
  * utm_info is flattened so you get real columns like device_category, utm_source, ...
  * custom_fields / order_customer_types are kept (as JSON text columns)
  * the COMPLETE order is also kept in raw_order, so nothing is ever lost and any
    brand-new field sticky.io adds later is still captured.

All business values are stored as STRING to avoid type clashes in sticky's messy
data. A handful of helper columns are typed: time_of_sale (TIMESTAMP),
date_of_sale (DATE), loaded_at (TIMESTAMP).
"""
import json
import logging
import re
from datetime import datetime, timezone
from typing import Dict, Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_TS_FORMAT = "%Y-%m-%d %H:%M:%S"

# These are represented by typed base columns, so don't duplicate them.
_BASE_KEYS = {"order_id", "time_stamp"}


def _parse_times(time_stamp, source_tz, report_tz):
    if not time_stamp:
        return None, None
    try:
        naive = datetime.strptime(time_stamp, _TS_FORMAT)
    except (ValueError, TypeError):
        logger.warning(f"Unparseable time_stamp: {time_stamp!r}")
        return None, None
    local = naive.replace(tzinfo=ZoneInfo(source_tz))
    utc = local.astimezone(timezone.utc)
    report_date = local.astimezone(ZoneInfo(report_tz)).date()
    return utc.isoformat(), report_date.isoformat()


def _safe_col(name: str) -> str:
    """Make a valid BigQuery column name from a sticky field name."""
    s = re.sub(r"[^A-Za-z0-9_]", "_", str(name))
    if not s or not re.match(r"[A-Za-z_]", s[0]):
        s = "_" + s
    return s[:300]


def _stringify(v):
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def order_to_flat_row(order, company, source, source_tz, report_tz, loaded_at_iso) -> Optional[Dict]:
    order_id = order.get("order_id")
    if order_id is None:
        logger.warning("Order without order_id skipped.")
        return None

    time_stamp = order.get("time_stamp")
    time_of_sale, date_of_sale = _parse_times(time_stamp, source_tz, report_tz)

    row = {
        "order_id": str(order_id),
        "company": company,
        "source": source,
        "time_stamp": time_stamp,
        "time_of_sale": time_of_sale,
        "date_of_sale": date_of_sale,
        "loaded_at": loaded_at_iso,
        "raw_order": json.dumps(order, ensure_ascii=False),
    }

    for key, value in order.items():
        if key in _BASE_KEYS:
            continue
        if key == "products":
            if isinstance(value, list) and value and isinstance(value[0], dict):
                for pk, pv in value[0].items():
                    row[f"prd_{pk}"] = _stringify(pv)
            row["products_json"] = _stringify(value)
        elif key == "totals_breakdown":
            if isinstance(value, dict):
                for bk, bv in value.items():
                    row[f"breakdown_{bk}"] = _stringify(bv)
            else:
                row["totals_breakdown_json"] = _stringify(value)
        elif key == "utm_info":
            if isinstance(value, dict):
                for uk, uv in value.items():
                    row[uk] = _stringify(uv)  # device_category, utm_source, ...
            row["utm_info_json"] = _stringify(value)
        else:
            row[key] = _stringify(value)

    # Ensure every column name is BigQuery-legal.
    return {_safe_col(k): v for k, v in row.items()}


# Backwards-compatible alias (older code imported order_to_row).
def order_to_row(order, company, source, source_tz, report_tz, loaded_at_iso):
    return order_to_flat_row(order, company, source, source_tz, report_tz, loaded_at_iso)
