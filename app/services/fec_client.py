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


_CONDUIT_SUBSTRINGS = ("WINRED", "ACTBLUE")


def _principal_committee_id(candidate_id: str, cycle: int | None = None) -> str | None:
    """Resolve the candidate's primary principal committee id.

    Filtering Schedule A by ``candidate_id`` surfaces every transaction
    where the candidate is linked anywhere — including joint fundraising
    committees and conduit passthroughs — which makes aggregated totals
    meaningless. Scoping to the candidate's own principal committee keeps
    the query to receipts the campaign actually took in.

    Uses ``/candidate/{id}/committees/?designation=P``; the base
    ``/candidate/{id}/`` record omits ``principal_committees``.
    """
    params: dict[str, Any] = {"designation": "P"}
    if cycle is not None:
        params["cycle"] = cycle
    data = _fec_get(f"/candidate/{candidate_id}/committees/", params)
    if not data:
        return None
    results = data.get("results") or []
    if not results:
        return None
    cid = results[0].get("committee_id")
    return cid or None


def get_top_pacs(
    candidate_id: str,
    cycle: int,
    n: int = 10,
    total_raised: float | None = None,
) -> list[dict]:
    """Return top ``n`` PAC contributors to the candidate's principal
    committee for ``cycle``.

    Uses Form 3 line ``11C`` (contributions from other political
    committees) scoped to the candidate's principal committee, which
    excludes loans, JFC transfers, in-kind from the candidate, and
    similar non-PAC receipts that dominate Schedule A by amount.
    Still drops memo duplicates and WinRed/ActBlue conduit rows as a
    belt-and-suspenders guard, aggregates by ``contributor_name``, and
    returns the top contributors as dicts with ``name``, ``amount``,
    ``state``.

    If ``total_raised`` is provided and any single aggregated PAC total
    exceeds it, the result is treated as contaminated (e.g. JFC bleed-
    through) and ``[]`` is returned with a warning logged.

    Errors, rate-limit exhaustion, and empty results all yield ``[]``.
    """
    if not candidate_id:
        return []
    try:
        committee_id = _principal_committee_id(candidate_id, cycle=cycle)
    except Exception as e:
        print(f"    [get_top_pacs principal-committee lookup failed: {e}]", flush=True)
        return []
    if not committee_id:
        return []

    try:
        data = _fec_get(
            "/schedules/schedule_a/",
            {
                "committee_id": committee_id,
                "two_year_transaction_period": cycle,
                "line_number": "F3-11C",
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
        if (r.get("memo_code") or "").upper() == "X":
            continue
        name = (r.get("contributor_name") or "").strip()
        if not name:
            continue
        upper = name.upper()
        if any(sub in upper for sub in _CONDUIT_SUBSTRINGS):
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

    if total_raised is not None and total_raised > 0:
        for name, v in agg.items():
            if v["amount"] > total_raised:
                print(
                    f"    [get_top_pacs WARNING: {name} aggregate "
                    f"${v['amount']:,.0f} exceeds total_raised "
                    f"${total_raised:,.0f} — dropping committee {committee_id}]",
                    flush=True,
                )
                return []

    top = sorted(agg.items(), key=lambda kv: -kv[1]["amount"])[:n]
    return [
        {"name": k, "amount": round(v["amount"], 2), "state": v["state"]}
        for k, v in top
    ]
