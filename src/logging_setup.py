"""
Structured JSON logging for Cloud Run.

Cloud Run automatically captures anything printed to stdout/stderr and shows it
in Cloud Logging. If we emit JSON, Cloud Logging parses the fields so you can
filter by severity, company, mode, etc. This makes debugging a large backfill
much easier than plain text.
"""
import json
import logging
import sys


class JsonFormatter(logging.Formatter):
    """Format every log line as a single JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            # Cloud Logging understands the "severity" field.
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        # Attach any extra context we passed via logger.info(msg, extra={...})
        for key in ("company", "mode", "source", "window", "orders"):
            value = getattr(record, key, None)
            if value is not None:
                entry[key] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure the root logger once and return it."""
    root = logging.getLogger()
    root.setLevel(level)
    # Remove any handlers added by libraries so we don't double-print.
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    return root
