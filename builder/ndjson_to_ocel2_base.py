import json
import re
from datetime import datetime, timezone
from collections import defaultdict

IN_PATH = "data/rw0/rw0_onchain_events.ndjson"
OUT_PATH = "data/rw0/rw0_native_ocel2.json"

# Keys we want to enforce as special types
FORCE_TIME_KEYS = {"ledger_time"}
FORCE_STRING_KEYS = {"source_tx", "actor_hash"}

def to_iso_z_from_payload_ts(ts_str: str) -> str:
    """
    Input example: "2024-10-01 07:39:45"
    Output:        "2024-10-01T07:39:45Z"
    Assumption: ts_str is already in UTC (or treated as UTC). Document this in the paper.
    """
    dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def normalize_object_id(obj_id: str) -> str:
    # "sr:104827" -> "sr-104827", "doc:399598" -> "doc-399598"
    return obj_id.replace(":", "-")

def infer_attr_type(name: str, value):
    if name in FORCE_TIME_KEYS:
        return "time"
    if name in FORCE_STRING_KEYS:
        return "string"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    # numeric strings -> integer (common in DB exports)
    if isinstance(value, str) and re.fullmatch(r"\d+", value):
        return "integer"
    # ISO time strings
    if isinstance(value, str):
        # very permissive; you can tighten if needed
        if value.endswith("Z") and "T" in value:
            return "time"
    return "string"

def normalize_attr_value(name: str, value):
    # Cast numeric strings to int except forced strings
    if name in FORCE_STRING_KEYS:
        return value
    if isinstance(value, str) and re.fullmatch(r"\d+", value):
        try:
            return int(value)
        except:
            return value
    return value

events_out = []
object_types_seen = set()
event_types_attrs = defaultdict(dict)  # eventType -> {attrName: attrType}

seen_event_ids = set()
n_lines = 0

with open(IN_PATH, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        n_lines += 1
        outer = json.loads(line)

        payload = json.loads(outer["payload_json"])

        event_id = outer.get("event_uid") or payload.get("event_uid")
        if event_id in seen_event_ids:
            # If this happens, switch to outer event_uid OR append tx_id
            event_id = f"{event_id}|{outer.get('tx_id','')}"
        seen_event_ids.add(event_id)

        event_type = payload["activity"]
        event_time = to_iso_z_from_payload_ts(payload["ts"])

        # attributes = vmap + ledger_time + source_tx
        vmap = payload.get("vmap", {}) or {}
        attrs = dict(vmap)
        attrs["ledger_time"] = outer.get("tx_time")
        attrs["source_tx"] = outer.get("tx_id")

        # build attributes list (deterministic order)
        attr_list = []
        for k in sorted(attrs.keys()):
            v = normalize_attr_value(k, attrs[k])
            attr_list.append({"name": k, "value": v})
            event_types_attrs[event_type][k] = infer_attr_type(k, v)

        # relationships from omap
        rels = []
        for o in payload.get("omap", []):
            otype = o["type"]
            oid = normalize_object_id(o["id"])
            object_types_seen.add(otype)
            rels.append({"objectId": oid, "qualifier": otype})

        events_out.append({
            "id": event_id,
            "type": event_type,
            "time": event_time,
            "attributes": attr_list,
            "relationships": rels
        })

# Build eventTypes / objectTypes sections
eventTypes_out = []
for etype in sorted(event_types_attrs.keys()):
    attrs = [{"name": k, "type": event_types_attrs[etype][k]} for k in sorted(event_types_attrs[etype].keys())]
    eventTypes_out.append({"name": etype, "attributes": attrs})

objectTypes_out = [{"name": ot, "attributes": []} for ot in sorted(object_types_seen)]

ocel2 = {
    "eventTypes": eventTypes_out,
    "objectTypes": objectTypes_out,
    "events": events_out
}

with open(OUT_PATH, "w", encoding="utf-8") as w:
    json.dump(ocel2, w, ensure_ascii=False, indent=2)

print(f"OK: read {n_lines} NDJSON lines -> wrote {len(events_out)} events to {OUT_PATH}")
print(f"Event types: {len(eventTypes_out)} | Object types: {len(objectTypes_out)}")