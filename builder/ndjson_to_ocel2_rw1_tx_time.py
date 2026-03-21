import json
from datetime import datetime

path = "data/rw1/rw1_onchain_events.ndjson"
times = []
n = 0

def parse_z(s):
    # "2026-01-22T22:19:18.885788Z"
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

with open(path, "r", encoding="utf-8") as f:
    for line in f:
        line=line.strip()
        if not line: 
            continue
        obj = json.loads(line)
        times.append(parse_z(obj["tx_time"]))
        n += 1

t0, t1 = min(times), max(times)
duration = (t1 - t0).total_seconds()
tps = n / duration if duration > 0 else None
print("n_tx =", n)
print("duration_s =", duration)
print("avg_TPS =", tps)