# SpatialText2SQL

The Spatial Text2SQL codebase is organized as a single root-level project with clear separation of responsibilities:

- `src/` contains reusable source code only
- `scripts/` contains executable utility scripts only
- `config/` contains runtime configuration files
- `data/` and `results/` store datasets and generated outputs

## Directory Overview

```text
.
├── config/                  # Evaluation and dataset configuration
├── scripts/
│   ├── evaluation/          # Pipeline entry scripts
│   └── spatialsql/          # SpatialSQL migration and validation scripts
├── src/
│   ├── datasets/            # Dataset loading and preprocessing
│   ├── evaluation/          # Metrics and report generation
│   ├── inference/           # Model loading and inference
│   ├── pipeline/            # Main pipeline orchestration
│   ├── preprocess/          # Data import tools
│   ├── prompting/           # Prompt construction
│   ├── retrieval/           # RAG and keyword retrieval
│   └── sql/                 # Schema and SQL dialect utilities
└── test/
```

## Common Entry Points

Run the evaluation pipeline:

```bash
python scripts/evaluation/run_pipeline.py --preprocess --inference --evaluate
```

Verify the SpatialSQL integration:

```bash
python scripts/spatialsql/verify_adaptation.py
```
