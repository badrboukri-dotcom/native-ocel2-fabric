"""Compute Temporal MAE for privacy overlay evaluation."""
import pm4py
import numpy as np
from datetime import datetime
import sys

ocel = pm4py.read.read_ocel2_json(sys.argv[1])
timestamps = ocel.events["ocel:timestamp"].apply(
    lambda x: x.timestamp() if hasattr(x, 'timestamp')
    else datetime.fromisoformat(str(x).replace("Z","+00:00")).timestamp()
).values

for delta_h, delta_s in [(1, 3600), (6, 21600)]:
    rounded = np.floor(timestamps / delta_s) * delta_s
    mae = np.mean(np.abs(timestamps - rounded))
    print(f"Temporal gen {delta_h}h: MAE = {mae:.0f} s ({mae/60:.0f} min)")
