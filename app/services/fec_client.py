"""FEC API client helpers.

Thin wrapper around the Federal Election Commission's public API
(https://api.open.fec.gov/). Exposes focused functions used by the
campaign-finance backfill pipeline.
"""

from __future__ import annotations

import os
import time
from typing import Any

import requests

FEC_BASE = "https://api.open.fec.gov/v1"


def _api_key() -> str:
    return os.getenv("FEC_API_KEY", "DEMO_KEY")


def _fec_get(path: str, params: dict | None = None, max_retries: int = 6) -> dict | None:
    """GET an FEC endpoint with retry on 429/5xx and network errors.

    Returns the parsed JSON body, or ``None`` if retries are exhausted.
    """
    p: dict[str, Any] = {"api_key": _api_key()}
    if params:
        p.update(params)
    url = f"{FEC_BASE}{path}"
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=p, timeout=60)
        except requests.RequestException as e:
            wait = 5 * (attempt + 1)
            print(f"    [fec network error {e}, retry in {wait}s]", flush=True)
            time.sleep(wait)
            continue
        if r.status_code == 429:
            wait = min(60 * (attempt + 1), 300)
            print(f"    [fec rate-limited, sleeping {wait}s]", flush=True)
            time.sleep(wait)
            continue
        if r.status_code >= 500:
            wait = 5 * (attempt + 1)
            print(f"    [fec {r.status_code}, retry in {wait}s]", flush=True)
            time.sleep(wait)
            continue
        if not r.ok:
            return None
        try:
            return r.json()
        except ValueError:
            return None
    return None


def get_top_pacs(candidate_id: str, cycle: int, n: int = 10) -> list[dict]:
    """Return top ``n`` PAC/committee contributors for a candidate in a cycle.

    Pulls Schedule A transactions with ``is_individual=false`` (which selects
    PACs and other committees), aggregates by ``contributor_name``, and
    returns the top contributors as dicts with ``name``, ``amount``, ``state``.

    Errors, rate-limit exhaustion, and empty results all yield ``[]``.
    """
    if not candidate_id:
        return []
    try:
        data = _fec_get(
            "/schedules/schedule_a/",
            {
                "candidate_id": candidate_id,
                "two_year_transaction_period": cycle,
                "is_individual": "false",
                "sort": "-contribution_receipt_amount",
                "per_page": 100,
            },
        )
    except Exception as e:
        print(f"    [get_top_pacs error: {e}]", flush=True)
        return []

    if not data:
        return []

    agg: dict[str, dict] = {}
    for r in data.get("results", []) or []:
        name = (r.get("contributor_name") or "").strip()
        if not name:
            continue
        try:
            amount = float(r.get("contribution_receipt_amount") or 0)
        except (TypeError, ValueError):
            continue
        if amount <= 0:
            continue
        state = (r.get("contributor_state") or "").strip().upper()
        cur = agg.setdefault(name, {"amount": 0.0, "state": state})
        cur["amount"] += amount
        if not cur["state"] and state:
            cur["state"] = state

    top = sorted(agg.items(), key=lambda kv: -kv[1]["amount"])[:n]
    return [
        {"name": k, "amount": round(v["amount"], 2), "state": v["state"]}
        for k, v in top
    ]
