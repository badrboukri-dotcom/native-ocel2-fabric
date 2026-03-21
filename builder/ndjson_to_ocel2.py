import json
from datetime import datetime, timezone

INP = "rw1_onchain_events.ndjson"
OUT = "rw1_native_ocel2_generated.json"

PREFIX_TO_TYPE = {
    "sr": "ServiceRequest",
    "doc": "Document",
    "wi": "WorkItem",
}

def norm_obj_id(raw: str) -> str:
    # "sr:104758" -> "sr-104758"
    return raw.replace(":", "-")

def parse_time(s: str) -> str:
    if not s:
        return s
    s = s.strip()

    # cas 1: déjà ISO avec Z
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        pass

    # cas 2: "YYYY-MM-DD HH:MM:SS" (comme dans payload_json.ts)
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return s  # fallback

events = []
event_types = set()
objects = {}  # objectId -> objectType

with open(INP, "r", encoding="utf-8") as f:
    for line_no, line in enumerate(f, 1):
        line = line.strip()
        if not line:
            continue

        rec = json.loads(line)

        # --- FIX: récupérer le payload depuis payload_json (string JSON) ---
        pj = rec.get("payload_json")
        if pj is None:
            raise ValueError(f"Ligne {line_no}: payload_json manquant")

        try:
            p = json.loads(pj) if isinstance(pj, str) else pj
        except Exception as e:
            raise ValueError(f"Ligne {line_no}: payload_json invalide: {e}")

        ev_id = p.get("event_uid")          # ex: "hd:406700" (pour matcher pm_events_rw1.tsv)
        ev_type = p.get("activity")         # ex: "DOC_Status_Valid"
        ev_time = p.get("ts")               # ex: "2024-10-01 07:39:45"

        if not ev_id or not ev_type or not ev_time:
            raise ValueError(
                f"Ligne {line_no}: event_uid/activity/ts manquant(s). "
                f"Trouvé event_uid={ev_id!r}, activity={ev_type!r}, ts={ev_time!r}"
            )

        event_types.add(ev_type)

        vmap = p.get("vmap") or {}

        attrs = []
        # actor_hash est dans vmap
        if vmap.get("actor_hash") is not None:
            attrs.append({"name": "actor_hash", "value": vmap.get("actor_hash")})
        # tx_time et tx_id sont au niveau racine
        if rec.get("tx_time") is not None:
            attrs.append({"name": "ledger_time", "value": parse_time(rec.get("tx_time"))})
        if rec.get("tx_id") is not None:
            attrs.append({"name": "source_tx", "value": rec.get("tx_id")})

        rels = []
        # omap est une liste d'objets {type,id} (pas une liste de strings)
        for obj in (p.get("omap") or []):
            if isinstance(obj, dict):
                raw_id = obj.get("id")
                otype = obj.get("type")
            else:
                # fallback si jamais omap contient des strings dans un autre dataset
                raw_id = str(obj)
                prefix = raw_id.split(":", 1)[0] if ":" in raw_id else raw_id.split("-", 1)[0]
                otype = PREFIX_TO_TYPE.get(prefix, prefix)

            if not raw_id:
                continue

            oid = norm_obj_id(raw_id)

            # si type absent, on déduit depuis le préfixe
            if not otype:
                prefix = raw_id.split(":", 1)[0] if ":" in raw_id else raw_id.split("-", 1)[0]
                otype = PREFIX_TO_TYPE.get(prefix, prefix)

            rels.append({"objectId": oid, "qualifier": otype})
            objects[oid] = otype

        events.append({
            "id": ev_id,
            "type": ev_type,
            "time": parse_time(ev_time),
            "attributes": attrs,
            "relationships": rels
        })

# tri reproductible (par time puis id)
events.sort(key=lambda e: (e.get("time", ""), e.get("id", "")))

object_types = [{"name": t, "attributes": []} for t in sorted(set(objects.values()))]

event_types_list = []
for t in sorted(event_types):
    event_types_list.append({
        "name": t,
        "attributes": [
            {"name": "actor_hash", "type": "string"},
            {"name": "ledger_time", "type": "time"},
            {"name": "source_tx", "type": "string"},
        ]
    })

objects_list = [{"id": oid, "type": otype, "attributes": []} for oid, otype in sorted(objects.items())]

ocel = {
    "eventTypes": event_types_list,
    "objectTypes": object_types,
    "events": events,
    "objects": objects_list,
}

with open(OUT, "w", encoding="utf-8") as w:
    json.dump(ocel, w, ensure_ascii=False, indent=2)

print(f"OK -> {OUT}")
print(f"  events:  {len(events)}")
print(f"  objects: {len(objects_list)}")
print(f"  eventTypes: {len(event_types_list)}")