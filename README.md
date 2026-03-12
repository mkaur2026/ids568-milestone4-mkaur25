# IDS 568 Milestone 4 – Distributed Feature Engineering with Ray

## Repository
ids568-milestone4-mkaur25

## Overview

This project implements a distributed feature engineering pipeline using **Ray** and compares it with a **local pandas baseline**.

The goal is to evaluate:

- performance differences between local and distributed execution
- runtime and memory usage
- distributed system overhead
- scalability of feature engineering pipelines

Synthetic transaction data is generated using a reproducible generator and processed using both execution modes.

---

# Project Structure

| File | Description |
|-----|-------------|
| generate_data.py | synthetic transaction dataset generator |
| pipeline.py | local and distributed feature engineering pipeline |
| README.md | project instructions |
| REPORT.md | benchmark results and analysis |
| requirements.txt | Python dependencies |
| .gitignore | prevents generated files from being committed |

---

# Environment Setup

Clone the repository:

```bash
git clone https://github.com/mkaur2026/ids568-milestone4-mkaur25.git
cd ids568-milestone4-mkaur25
```

Create a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

# Synthetic Data Generation

The dataset generator supports deterministic synthetic data creation using a fixed random seed.

Each generated record contains:

- user_id
- amount
- timestamp
- category

Example generation commands:

### Small dataset (testing)

```bash
python generate_data.py --rows 1000 --seed 42 --output test_data --partitions 4
```

### Medium dataset

```bash
python generate_data.py --rows 100000 --seed 42 --output data_100k --partitions 8
```

### Large dataset

```bash
python generate_data.py --rows 1000000 --seed 42 --output data_1m --partitions 8
```

### Extra large dataset

```bash
python generate_data.py --rows 10000000 --seed 42 --output data_10m --partitions 16
```

The generator supports datasets exceeding **10 million rows**.

---

# Feature Engineering Pipeline

The pipeline computes aggregated features per user.

Computed features:

- total_amount
- avg_amount
- tx_count
- avg_log_amount

Processing steps:

1. load parquet partitions  
2. filter transactions where amount > 0  
3. compute `log_amount = log1p(amount)`  
4. group by user_id  
5. aggregate features  

---

# Running the Pipeline

## Local Execution

```bash
python pipeline.py --input data_100k --output results_local_100k --mode local
```

## Distributed Execution

```bash
python pipeline.py --input data_100k --output results_dist_100k --mode distributed --batches 4
```

The distributed pipeline uses **Ray workers** to process data batches in parallel.

---

# Output

Each run produces two files:

| File | Description |
|----|-------------|
| features.parquet | engineered user-level features |
| metrics.json | runtime and memory statistics |

Metrics include:

- runtime
- memory usage
- partition count
- estimated shuffle volume
- worker utilization

---

# Reproducibility

Synthetic data generation is deterministic using a fixed random seed.

Example:

```bash
python generate_data.py --rows 1000 --seed 42 --output run1 --partitions 4
python generate_data.py --rows 1000 --seed 42 --output run2 --partitions 4
diff -r run1 run2
```

If `diff` produces no output, the dataset is reproducible.

---

# Benchmarking

The pipeline was tested with datasets of increasing size:

- 1K rows
- 100K rows
- 1M rows
- 10M rows

Results and analysis are documented in **REPORT.md**.

---

# Key Observations

- Local pandas execution was faster for all tested dataset sizes on a single laptop.
- Distributed Ray execution introduced overhead from worker initialization and task scheduling.
- Distributed systems become beneficial for larger workloads or multi-node environments.

---

# Submission

Final submission uses the repository tag:

```
submission
```

Repository link submitted:

```
https://github.com/mkaur2026/ids568-milestone4-mkaur25
```
