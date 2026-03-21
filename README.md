# Native OCEL 2.0 Generation on Permissioned Blockchains

**Replication package** for the paper:

> B. Boukri, H. Sbaï, "Native OCEL 2.0 Generation on Permissioned Blockchains for Privacy-Preserving Object-Centric Process Mining," submitted to *Array* (Elsevier), 2025.

## Overview

This repository provides the source code, analysis scripts, privacy overlays, and evaluation results for generating OCEL 2.0 event records **natively** from Hyperledger Fabric smart contracts, with policy-driven privacy overlays and on-chain auditability.

## Repository Structure

```
├── chaincode/          # Hyperledger Fabric chaincode (TypeScript)
│   └── src/ocelEmission.ts   # Native OCEL 2.0 event emission
├── builder/            # Off-chain OCEL builder scripts
│   ├── ndjson_to_ocel2.py          # NDJSON → OCEL 2.0 converter
│   ├── ndjson_to_ocel2_rw1.py      # RW-1 specific variant
│   └── ndjson_to_ocel2_rw1_tx_time.py
├── analysis/           # PM4Py analysis pipeline
│   ├── run_ocel2_analysis_v2.py    # Discovery, conformance, bootstrap
│   └── requirements.txt
├── overlays/           # Privacy overlay implementations
│   └── privacy_overlays.py   # Temporal gen, k-anon, suppression
├── evaluation/         # Results and aggregated statistics
│   └── rw1/
│       ├── full_results.json       # PM4Py output (fitness, precision, F1)
│       └── latex_tables.tex        # LaTeX-ready tables
└── paper/figures/      # TikZ figures used in the paper
```

## Prerequisites

| Component | Version |
|-----------|---------|
| Hyperledger Fabric | v2.5.14 |
| Docker + Compose | 27.3.x / v2.29.x |
| Node.js | 18.x |
| Python | 3.10+ |
| PM4Py | 2.7.x |

## Quick Start

### 1. Install analysis dependencies

```bash
cd analysis
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Run discovery and conformance analysis

```bash
python run_ocel2_analysis_v2.py <path_to_ocel2.json> \
  --output-dir results --n-runs 30
```

This produces:
- `full_results.json` — fitness, precision, F1 (native vs. ETL), Wilcoxon test
- `latex_tables.tex` — copy-paste-ready LaTeX tables

### 3. Apply privacy overlays

```python
from overlays.privacy_overlays import compose_overlays, compute_info_loss
import json

ocel = json.load(open("my_ocel2.json"))
overlaid = compose_overlays(ocel, delta_seconds=3600, k=5,
                            suppress_keys=["actor_hash", "source_tx"])
info_loss = compute_info_loss(ocel, overlaid)
print(info_loss)
```

### 4. Deploy chaincode (requires Fabric test-network)

```bash
cd $FABRIC_SAMPLES/test-network
./network.sh up createChannel -c mychannel
./network.sh deployCC -ccn ocel -ccp ../native-ocel2-fabric/chaincode -ccl typescript
```

## Datasets

| ID | Events | Objects | Event types | Object types | Period |
|----|--------|---------|-------------|--------------|--------|
| RW-1 | 141,998 | 78,175 | 21 | 3 | Oct 2024 – Sep 2025 |
| RW-0 | 11,852 | 6,649 | 20 | 3 | (temporal slice) |

> **Note:** The anonymised operational logs cannot be shared publicly due to confidentiality constraints. Aggregated statistics and the reproducible extraction logic are provided in `evaluation/`.

## Key Results

| Metric | Native OCEL | ETL baseline | Δ |
|--------|-------------|--------------|---|
| Fitness | 1.000 | 1.000 | +0.0 pp |
| Precision | 0.956 | 0.948 | +0.8 pp |
| F1 | 0.978 | 0.973 | **+0.5 pp*** |
| Nodes | 258 | 303 | −15% |
| Arcs | 332 | 392 | −15% |

\* Wilcoxon signed-rank test, *p* < 0.001

## Citation

```bibtex
@article{boukri2025-native-ocel,
  author  = {Boukri, Badr and Sba{\"i}, Hanae},
  title   = {Native {OCEL}~2.0 Generation on Permissioned Blockchains
             for Privacy-Preserving Object-Centric Process Mining},
  journal = {Array},
  year    = {2025},
  note    = {Submitted}
}
```

## License

MIT — see [LICENSE](LICENSE).
