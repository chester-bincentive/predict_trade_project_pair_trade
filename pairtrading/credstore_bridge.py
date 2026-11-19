"""Resolve the live Polymarket signing key from the optional company credstore."""
from __future__ import annotations

import os
import re

_RAW_HEX64 = re.compile(r"[0-9a-fA-F]{64}")
_PRIVATE_KEY = re.compile(r"0x[0-9a-fA-F]{64}")


def fetch_private_key() -> str:
    region_name = os.environ.get("CREDSTORE_REGION", "").strip()
    secret_name = os.environ.get("CREDSTORE_SECRET_NAME", "").strip()
    try:
        if not region_name or not secret_name:
            raise ValueError("credstore settings are not configured")
        from credstore import CredstoreClient

        payload = CredstoreClient(region_name=region_name).fetch_json(secret_name)
        value = payload.get("private_key") if isinstance(payload, dict) else None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("private_key missing or empty")
        value = value.strip()
        if _RAW_HEX64.fullmatch(value):
            value = "0x" + value
        if not _PRIVATE_KEY.fullmatch(value):
            raise ValueError("private_key has invalid format")
        return value
    except Exception as exc:
        raise RuntimeError(f"credstore private-key fetch failed: {type(exc).__name__}") from exc
