"""
Privacy overlays for OCEL 2.0 logs.

Implements the three overlays formalised in Section 3.3 of the paper:
  - Temporal generalisation (G_delta)
  - Role k-anonymity (K_k)
  - Attribute suppression (S_S)
"""
import json
import math
from collections import Counter
from copy import deepcopy
from datetime import datetime
from typing import Any


def temporal_generalisation(ocel: dict, delta_seconds: int) -> dict:
    """Round all timestamps to delta-sized buckets.

    G_delta: time'(e) = floor(time(e) / delta) * delta
    """
    ocel = deepcopy(ocel)
    for eid, ev in ocel.get("events", {}).items():
        ts = ev.get("time", "")
        if ts:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            epoch = dt.timestamp()
            rounded = math.floor(epoch / delta_seconds) * delta_seconds
            ev["time"] = datetime.fromtimestamp(
                rounded, tz=dt.tzinfo
            ).isoformat()
    return ocel


def role_k_anonymity(ocel: dict, k: int, qi_key: str = "actor_hash") -> dict:
    """Enforce k-anonymity on quasi-identifier attribute.

    Groups actors into equivalence classes of size >= k
    by replacing infrequent values with a generalised label.
    """
    ocel = deepcopy(ocel)
    counts: Counter = Counter()
    for ev in ocel.get("events", {}).values():
        val = ev.get("attributes", {}).get(qi_key)
        if val:
            counts[val] += 1

    # Build suppression set: actors appearing < k times
    suppress = {v for v, c in counts.items() if c < k}

    for ev in ocel.get("events", {}).values():
        attrs = ev.get("attributes", {})
        if attrs.get(qi_key) in suppress:
            attrs[qi_key] = f"group_lt_{k}"
    return ocel


def attribute_suppression(ocel: dict, sensitive_keys: list[str]) -> dict:
    """Remove sensitive keys from all event attributes.

    S_S: forall e, forall k in S: vmap'(e)(k) is undefined.
    """
    ocel = deepcopy(ocel)
    for ev in ocel.get("events", {}).values():
        attrs = ev.get("attributes", {})
        for key in sensitive_keys:
            attrs.pop(key, None)
    return ocel


def compose_overlays(
    ocel: dict,
    delta_seconds: int = 3600,
    k: int = 5,
    suppress_keys: list[str] | None = None,
) -> dict:
    """Apply composed disclosure: O_c = S_S . K_k . G_delta"""
    log = temporal_generalisation(ocel, delta_seconds)
    log = role_k_anonymity(log, k)
    if suppress_keys:
        log = attribute_suppression(log, suppress_keys)
    return log


def compute_info_loss(original: dict, transformed: dict) -> dict:
    """Compute information loss metrics between original and overlaid logs."""
    orig_ts = {ev.get("time") for ev in original.get("events", {}).values()}
    trans_ts = {ev.get("time") for ev in transformed.get("events", {}).values()}
    ts_reduction = 1.0 - len(trans_ts) / max(len(orig_ts), 1)

    orig_actors = {
        ev.get("attributes", {}).get("actor_hash")
        for ev in original.get("events", {}).values()
    }
    trans_actors = {
        ev.get("attributes", {}).get("actor_hash")
        for ev in transformed.get("events", {}).values()
    }
    actor_reduction = 1.0 - len(trans_actors) / max(len(orig_actors), 1)

    return {
        "distinct_timestamps_reduction_pct": round(ts_reduction * 100, 1),
        "distinct_actors_reduction_pct": round(actor_reduction * 100, 1),
    }
