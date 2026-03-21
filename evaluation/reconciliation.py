import csv, json
from datetime import datetime, timezone

NDJSON = "rw1_onchain_events.ndjson"
PM_EVENTS = "pm_events_rw1.tsv"
OUT = "reconciliation_events.csv"
def parse_pm_ts(ts: str):
    if not ts:
        return None
    # ex: "2024-10-01 07:29:05" (pas de TZ) -> on suppose UTC
    dt = datetime.strptime(ts.strip(), "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=timezone.utc)

def parse_ocel_ts(ts: str):
    if not ts:
        return None
    s = ts.strip()
    # ex: "2024-10-01T07:29:05Z" -> compatible fromisoformat via +00:00
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    # si jamais encore naïf, on force UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt

def load_payload(line_obj: dict) -> dict:
    pj = line_obj.get("payload_json")
    if isinstance(pj, str) and pj:
        return json.loads(pj)
    if isinstance(pj, dict):
        return pj
    p = line_obj.get("payload")
    if isinstance(p, dict):
        pj2 = p.get("payload_json")
        if isinstance(pj2, str) and pj2:
            return json.loads(pj2)
        if isinstance(pj2, dict):
            return pj2
        return p
    return {}

def parse_pm_time(s: str):
    # "2024-10-01 07:29:05" -> UTC
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

def parse_any_time(s: str):
    if not s:
        return None
    s = s.strip()
    # cas ISO 8601
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        pass
    # cas "YYYY-MM-DD HH:MM:SS" (comme dans payload_json.ts)
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None

onchain = {}
with open(NDJSON, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        root = json.loads(line)
        payload = load_payload(root)

        # IMPORTANT: alignement PM
        eid = payload.get("event_uid")
        if not eid:
            continue

        vmap = payload.get("vmap") or {}
        onchain[eid] = {
            "activity": payload.get("activity"),
            "ts": payload.get("ts"),
            "actor_hash": vmap.get("actor_hash"),
            "sr_id": vmap.get("sr_id"),
            "doc_id": vmap.get("doc_id"),
            "hist_id": vmap.get("hist_id"),
            "service_id": vmap.get("service_id"),
        }

rows = []
missing_onchain = 0
mismatch_activity = 0
mismatch_actor = 0
mismatch_time = 0

with open(PM_EVENTS, "r", encoding="utf-8") as f:
    for line_no, line in enumerate(f, 1):
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 3:
            continue

        eid = parts[0]
        pm_time_s = parts[1]
        pm_activity = parts[2]
        pm_actor = parts[-1] if len(parts) >= 1 else None

        oc = onchain.get(eid)
        if not oc:
            missing_onchain += 1
            rows.append([eid, "MISSING_IN_ONCHAIN", pm_activity, None, pm_actor, None, None])
            continue

        status = "OK"

        if pm_activity != oc.get("activity"):
            status = "MISMATCH_ACTIVITY"
            mismatch_activity += 1

        if pm_actor and oc.get("actor_hash") and pm_actor != oc.get("actor_hash"):
            status = "MISMATCH_ACTOR" if status == "OK" else status + "+ACTOR"
            mismatch_actor += 1

        #dt_pm = parse_pm_time(pm_time_s)
        #dt_oc = parse_any_time(oc.get("ts"))
        dt_pm = parse_pm_ts(pm_time_s)
        dt_oc = parse_ocel_ts(oc.get("ts"))

        delta_s = abs((dt_pm - dt_oc).total_seconds()) if (dt_pm and dt_oc) else None

        if delta_s is not None and delta_s > 0:
            status = "MISMATCH_TIME" if status == "OK" else status + "+TIME"
            mismatch_time += 1

        rows.append([eid, status, pm_activity, oc.get("activity"), pm_actor, oc.get("actor_hash"), delta_s])

with open(OUT, "w", encoding="utf-8", newline="") as w:
    wr = csv.writer(w)
    wr.writerow(["event_id","status","pm_activity","onchain_activity","pm_actor_hash","onchain_actor_hash","abs_time_delta_s"])
    wr.writerows(rows)

print(f"OK -> {OUT}")
print("Résumé:")
print(f"  lignes pm_events sans event on-chain correspondant: {missing_onchain}")
print(f"  mismatches activity: {mismatch_activity}")
print(f"  mismatches actor_hash: {mismatch_actor}")
print(f"  mismatches time (delta>0s): {mismatch_time}")