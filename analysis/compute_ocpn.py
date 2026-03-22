"""Discover OCPN and compute multi-object statistics."""
import pm4py
import sys

ocel = pm4py.read.read_ocel2_json(sys.argv[1])
result = pm4py.discover_oc_petri_net(ocel)

total_p, total_t, total_a = 0, 0, 0
for ot, pn_data in result["petri_nets"].items():
    net = pn_data[0] if isinstance(pn_data, tuple) else pn_data
    p, t, a = len(net.places), len(net.transitions), len(net.arcs)
    print(f"  {ot}: places={p}, transitions={t}, arcs={a}")
    total_p += p; total_t += t; total_a += a

print(f"\nOCPN totals: places={total_p}, transitions={total_t}, arcs={total_a}")

rels = ocel.relations
types_per_event = rels.groupby("ocel:eid")["ocel:type"].nunique()
multi = int((types_per_event >= 2).sum())
print(f"Events with >=2 OT: {multi} ({100*multi/len(ocel.events):.1f}%)")
print(f"Mean OT per event: {types_per_event.mean():.2f}")
