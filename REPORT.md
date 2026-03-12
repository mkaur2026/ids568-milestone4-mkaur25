# Distributed Feature Engineering Benchmark Report

## 1. Objective

The objective of this project is to implement and evaluate a distributed feature engineering pipeline using Ray and compare it against a local pandas baseline.

The goal is to understand:

• runtime differences between local and distributed execution  
• memory usage differences  
• overhead introduced by distributed frameworks  
• conditions under which distributed systems become beneficial  

Synthetic transaction data is generated using a reproducible generator and processed using both local and distributed pipelines.

---

# 2. Data Generation

The dataset is generated using `generate_data.py`.

Key characteristics:

• deterministic generation using a fixed random seed  
• configurable row counts  
• configurable partition counts  
• skewed user distribution to simulate real-world workloads  

Each generated record contains:

| Column | Description |
|------|-------------|
| user_id | synthetic user identifier |
| amount | transaction amount |
| timestamp | transaction timestamp |
| category | transaction category |

Example generation command:

python generate_data.py --rows 10000000 --seed 42 --output data_10m --partitions 16

The generator supports datasets exceeding **10 million rows**, satisfying the assignment requirement.

---

# 3. Feature Engineering Pipeline

The feature engineering pipeline computes aggregated statistics per user.

Computed features:

| Feature | Description |
|-------|-------------|
| total_amount | total transaction amount per user |
| avg_amount | average transaction amount |
| tx_count | number of transactions |
| avg_log_amount | mean of log1p(amount) |

The pipeline performs the following steps:

1. Load parquet partitions
2. Filter transactions where amount > 0
3. Compute log_amount = log1p(amount)
4. Group by user_id
5. Aggregate features

---

# 4. Execution Modes

## Local Mode

Local execution uses pandas.

Steps:

1. load all parquet partitions
2. concatenate into a dataframe
3. compute transformations
4. group and aggregate

This represents the **single-machine baseline**.

---

## Distributed Mode

Distributed execution uses **Ray**.

Steps:

1. split dataframe into batches
2. execute batch processing using @ray.remote
3. compute partial aggregations
4. merge partial results into final output

Parallel workers process batches simultaneously.

---

# 5. Correctness Verification

Local and distributed outputs were directly compared.

Verification results:

Same shape: True  
Same user_id order: True  
total_amount: True  
avg_amount: True  
tx_count: True  
avg_log_amount: True  

This confirms that the distributed pipeline produces **deterministic and identical results** compared to the local baseline.

---

# 6. Benchmark Methodology

Benchmarks were executed locally on a MacBook.

The following metrics were collected:

• wall-clock runtime  
• process memory usage  
• partitions used  
• estimated shuffle volume  
• estimated worker utilization  

Datasets tested:

| Dataset | Rows |
|------|------|
| Small | 1,000 |
| Medium | 100,000 |
| Large | 1,000,000 |
| Extra Large | 10,000,000 |

---

# 7. Benchmark Results

## 7.1 Small Dataset (1,000 rows)

| Mode | Runtime (sec) | Memory Delta (GB) | Partitions | Shuffle Est (MB) | Worker Utilization |
|-----|--------------|------------------|------------|------------------|------------------|
| Local | 0.010 | 0.002 | 1 | 0.00 | 0% |
| Distributed | 0.366 | 0.006 | 4 | 0.05 | 100% |

Observation:

Distributed execution is significantly slower due to framework startup overhead.

---

## 7.2 Medium Dataset (100,000 rows)

| Mode | Runtime (sec) | Memory Delta (GB) | Partitions | Shuffle Est (MB) | Worker Utilization |
|-----|--------------|------------------|------------|------------------|------------------|
| Local | 0.016 | 0.013 | 1 | 0.00 | 0% |
| Distributed | 0.392 | 0.025 | 4 | 4.53 | 100% |

Observation:

The local baseline still outperforms distributed execution.

Distributed task coordination dominates computation time.

---

## 7.3 Large Dataset (1,000,000 rows)

| Mode | Runtime (sec) | Memory Delta (GB) | Partitions | Shuffle Est (MB) | Worker Utilization |
|-----|--------------|------------------|------------|------------------|------------------|
| Local | 0.091 | 0.138 | 1 | 0.00 | 0% |
| Distributed | 0.437 | 0.156 | 4 | 45.29 | 100% |

Observation:

Even at 1M rows, the overhead of distributed processing exceeds the computational savings.

---

## 7.4 Extra Large Dataset (10,000,000 rows)

| Mode | Runtime (sec) | Memory Delta (GB) | Partitions | Shuffle Est (MB) | Worker Utilization |
|-----|--------------|------------------|------------|------------------|------------------|
| Local | 1.493 | 0.268 | 1 | 0.00 | 0% |
| Distributed | 2.134 | 0.454 | 4 | 460.54 | 100% |

Observation:

Local pandas execution still outperforms Ray on a single laptop.

However, distributed execution shows significantly higher memory consumption and shuffle volume.

---

# 8. Performance Analysis

## Distributed Overhead

Distributed frameworks introduce several sources of overhead:

• worker initialization  
• task scheduling  
• data serialization  
• batch aggregation coordination  

For relatively simple computations, these overheads dominate runtime.

---

## When Distributed Systems Help

Distributed systems become beneficial when:

• dataset size exceeds single-machine memory  
• workloads are computationally expensive  
• many independent tasks can run in parallel  
• cluster infrastructure is available  

In these cases, parallel scaling can offset coordination overhead.

---

# 9. Reliability and Cost Considerations

## Reliability

Advantages of distributed pipelines:

• improved scalability  
• fault isolation across workers  
• ability to process extremely large datasets  

Challenges:

• more complex system architecture  
• additional failure modes  
• coordination overhead

---

## Cost

Distributed systems introduce additional costs:

• infrastructure cost  
• higher memory usage  
• scheduling overhead  
• potential network transfer cost

For smaller workloads, local processing is usually more cost-efficient.

---

# 10. Production Recommendation

Based on these results:

Local execution is preferable for small and moderate workloads that comfortably fit within a single machine.

Distributed execution becomes beneficial when:

• data size exceeds local memory  
• workloads become computationally expensive  
• horizontal scalability is required  
• fault tolerance is important.

---

# 11. Conclusion

This project demonstrates that distributed processing is not inherently faster than local execution.

For this workload on a single machine:

• pandas consistently outperformed Ray  
• distributed overhead dominated computation time  

However, Ray provides a scalable architecture suitable for large-scale distributed workloads and production data pipelines.
