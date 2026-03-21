#!/usr/bin/env python3
"""
OCEL 2.0 Analysis Pipeline — Native OCEL on Permissioned Blockchains
=====================================================================
INSTALL FIRST:  pip3 install pm4py pandas numpy scipy
USAGE:          python3 run_ocel2_analysis_v2.py <ocel2.json> --output-dir results --n-runs 30
"""
import sys, os, json, time, copy, warnings, argparse
missing = []
for pkg in ['numpy','pandas','scipy','pm4py']:
    try: __import__(pkg)
    except ImportError: missing.append(pkg)
if missing:
    print(f"\n  ERROR: pip3 install {' '.join(missing)}\n"); sys.exit(1)
import numpy as np, pandas as pd
from scipy import stats
import pm4py
warnings.filterwarnings("ignore")
parser = argparse.ArgumentParser()
parser.add_argument("ocel_path"); parser.add_argument("--output-dir", default="results")
parser.add_argument("--n-runs", type=int, default=10); parser.add_argument("--seed", type=int, default=42)
args = parser.parse_args()
os.makedirs(args.output_dir, exist_ok=True); np.random.seed(args.seed)
print(f"Loading {args.ocel_path} ...")
ocel = pm4py.read.read_ocel2_json(args.ocel_path)
n_ev = len(ocel.events); n_ob = len(ocel.objects); n_rel = len(ocel.relations)
ot_list = list(ocel.objects['ocel:type'].value_counts().index)
print(f"  {n_ev:,} events, {n_ob:,} objects, {n_rel:,} links, {len(ot_list)} OTs: {ot_list}")
def analyze(od, lbl=""):
    res = {}
    for ot in ot_list:
        fl = pm4py.ocel.ocel_flattening(od, ot)
        if fl is None or len(fl) < 3: continue
        nc = fl['case:concept:name'].nunique()
        net,im,fm = pm4py.discover_petri_net_inductive(fl)
        fi = pm4py.fitness_token_based_replay(fl,net,im,fm).get('average_trace_fitness',0)
        pr = pm4py.precision_token_based_replay(fl,net,im,fm)
        pr = pr if isinstance(pr,(int,float)) else 0.0
        nd = len(net.places)+len(net.transitions); ar = len(net.arcs)
        res[ot] = {'fit':fi,'prec':pr,'cases':nc,'nodes':nd,'arcs':ar}
        if lbl: print(f"  [{lbl}] {ot}: fit={fi:.4f} prec={pr:.4f} cases={nc}")
    tc = sum(r['cases'] for r in res.values())
    wf = sum(r['fit']*r['cases']/tc for r in res.values())
    wp = sum(r['prec']*r['cases']/tc for r in res.values())
    tn = sum(r['nodes'] for r in res.values()); ta = sum(r['arcs'] for r in res.values())
    return {'fitness':wf,'precision':wp,'f1':2*wf*wp/(wf+wp) if wf+wp>0 else 0,'nodes':tn,'arcs':ta}
nat = analyze(ocel,"Native")
print(f"\nNATIVE: fit={nat['fitness']:.4f} prec={nat['precision']:.4f} F1={nat['f1']:.4f} nodes={nat['nodes']} arcs={nat['arcs']}")
# ETL
oe = copy.deepcopy(ocel); ev = oe.events.copy(); rl = oe.relations.copy(); n=len(ev)
di = np.random.choice(ev.index,int(n*0.03),replace=True)
de = ev.loc[di].copy(); de['ocel:eid'] = de['ocel:eid'].apply(lambda x:x+'_dup')
ev = pd.concat([ev,de],ignore_index=True)
for eid in ocel.events.loc[di,'ocel:eid']:
    b = rl[rl['ocel:eid']==eid].copy(); b['ocel:eid']=eid+'_dup'; rl = pd.concat([rl,b],ignore_index=True)
k = np.random.random(len(ev))>0.02; dr=set(ev[~k]['ocel:eid'])
oe.events = ev[k].reset_index(drop=True); oe.relations = rl[~rl['ocel:eid'].isin(dr)].reset_index(drop=True)
etl = analyze(oe,"ETL")
print(f"ETL:    fit={etl['fitness']:.4f} prec={etl['precision']:.4f} F1={etl['f1']:.4f} nodes={etl['nodes']} arcs={etl['arcs']}")
print(f"Delta F1: {(nat['f1']-etl['f1'])*100:+.2f} pp, Node reduction: {(1-nat['nodes']/etl['nodes'])*100:.1f}%")
# Bootstrap
N=args.n_runs; print(f"\nBootstrap ({N} runs)...")
def brun(od):
    fs,ps,ws = [],[],[]
    for ot in ot_list:
        fl = pm4py.ocel.ocel_flattening(od,ot)
        if fl is None or len(fl)<5: continue
        ci = fl['case:concept:name'].unique(); si = np.random.choice(ci,len(ci),replace=True)
        sl = fl[fl['case:concept:name'].isin(set(si))].copy()
        if len(sl)<5: continue
        ne,im,fm = pm4py.discover_petri_net_inductive(sl)
        fi = pm4py.fitness_token_based_replay(sl,ne,im,fm).get('average_trace_fitness',0)
        pr = pm4py.precision_token_based_replay(sl,ne,im,fm); pr = pr if isinstance(pr,(int,float)) else 0.0
        fs.append(fi); ps.append(pr); ws.append(len(ci))
    if not fs: return None
    w = np.array(ws,dtype=float); w/=w.sum(); f=np.average(fs,weights=w); p=np.average(ps,weights=w)
    return {'fit':f,'prec':p,'f1':2*f*p/(f+p) if f+p>0 else 0}
nr,er = [],[]
for i in range(N):
    a=brun(ocel); b=brun(oe)
    if a: nr.append(a)
    if b: er.append(b)
    if (i+1)%(max(1,N//5))==0: print(f"  {i+1}/{N}")
ci = lambda v: (np.mean(v),np.percentile(v,2.5),np.percentile(v,97.5))
nf=[r['f1'] for r in nr]; ef=[r['f1'] for r in er]
nfm,nfl,nfh=ci(nf); efm,efl,efh=ci(ef)
mn=min(len(nf),len(ef))
ws,wp = stats.wilcoxon(nf[:mn],ef[:mn]) if mn>=10 else (0,1.0)
print(f"Native F1: {nfm:.4f} [{nfl:.4f},{nfh:.4f}]")
print(f"ETL    F1: {efm:.4f} [{efl:.4f},{efh:.4f}]")
print(f"Wilcoxon p={wp:.6f}")
# Save
r = {'native':nat,'etl':etl,'bootstrap':{'native_f1':f"{nfm:.4f}[{nfl:.4f},{nfh:.4f}]",
    'etl_f1':f"{efm:.4f}[{efl:.4f},{efh:.4f}]",'wilcoxon_p':wp},'n_events':n_ev,'n_objects':n_ob,'n_links':n_rel}
json.dump(r,open(os.path.join(args.output_dir,'full_results.json'),'w'),indent=2,default=str)
nfi=[x['fit'] for x in nr];npr=[x['prec'] for x in nr];efi=[x['fit'] for x in er];epr=[x['prec'] for x in er]
with open(os.path.join(args.output_dir,'latex_tables.tex'),'w') as f:
    nim,nil,nih=ci(nfi); npm,npl,nph=ci(npr); eim,eil,eih=ci(efi); epm,epl,eph=ci(epr)
    f.write(f"Native OCEL & {nim:.3f} [{nil:.3f}, {nih:.3f}] & {npm:.3f} [{npl:.3f}, {nph:.3f}] & {nfm:.3f} [{nfl:.3f}, {nfh:.3f}] \\\\\n")
    f.write(f"ETL baseline & {eim:.3f} [{eil:.3f}, {eih:.3f}] & {epm:.3f} [{epl:.3f}, {eph:.3f}] & {efm:.3f} [{efl:.3f}, {efh:.3f}] \\\\\n")
    f.write(f"% Wilcoxon p={wp:.6f}  Nodes: native={nat['nodes']} etl={etl['nodes']}  Arcs: native={nat['arcs']} etl={etl['arcs']}\n")
print(f"\nSaved: {args.output_dir}/full_results.json + latex_tables.tex")
print("DONE")
