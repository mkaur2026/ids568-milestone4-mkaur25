import argparse
import json
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
import psutil
import ray


def load_parquet_folder(input_path: str) -> pd.DataFrame:
    """
    Load all parquet files from a folder into one pandas DataFrame.
    """
    folder = Path(input_path)
    parquet_files = sorted(folder.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {input_path}")

    frames = [pd.read_parquet(file) for file in parquet_files]
    df = pd.concat(frames, ignore_index=True)
    return df


def local_feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Local baseline using pandas.
    """
    df = df.copy()
    df = df[df["amount"] > 0]
    df["log_amount"] = np.log1p(df["amount"])

    result = (
        df.groupby("user_id")
        .agg(
            total_amount=("amount", "sum"),
            avg_amount=("amount", "mean"),
            tx_count=("amount", "count"),
            avg_log_amount=("log_amount", "mean"),
        )
        .reset_index()
        .sort_values("user_id")
        .reset_index(drop=True)
    )

    return result


@ray.remote
def process_batch(batch_df: pd.DataFrame) -> pd.DataFrame:
    """
    Process one batch in parallel using Ray.
    """
    batch_df = batch_df.copy()
    batch_df = batch_df[batch_df["amount"] > 0]
    batch_df["log_amount"] = np.log1p(batch_df["amount"])

    grouped = (
        batch_df.groupby("user_id")
        .agg(
            total_amount=("amount", "sum"),
            amount_sum_for_avg=("amount", "sum"),
            tx_count=("amount", "count"),
            log_amount_sum=("log_amount", "sum"),
        )
        .reset_index()
    )

    return grouped


def distributed_feature_engineering(df: pd.DataFrame, n_batches: int = 4) -> pd.DataFrame:
    """
    Distributed execution using Ray.
    """
    batches = [df.iloc[i::n_batches].copy() for i in range(n_batches)]
    futures = [process_batch.remote(batch) for batch in batches]
    partial_results = ray.get(futures)

    combined = pd.concat(partial_results, ignore_index=True)

    final_result = (
        combined.groupby("user_id")
        .agg(
            total_amount=("total_amount", "sum"),
            amount_sum_for_avg=("amount_sum_for_avg", "sum"),
            tx_count=("tx_count", "sum"),
            log_amount_sum=("log_amount_sum", "sum"),
        )
        .reset_index()
    )

    final_result["avg_amount"] = final_result["amount_sum_for_avg"] / final_result["tx_count"]
    final_result["avg_log_amount"] = final_result["log_amount_sum"] / final_result["tx_count"]

    final_result = final_result[
        ["user_id", "total_amount", "avg_amount", "tx_count", "avg_log_amount"]
    ].sort_values("user_id").reset_index(drop=True)

    return final_result


def benchmark(func, *args, **kwargs):
    """
    Measure runtime and memory usage for a function call.
    """
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / (1024 ** 3)

    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start_time

    mem_after = process.memory_info().rss / (1024 ** 3)

    metrics = {
        "wall_time_sec": round(elapsed, 3),
        "memory_before_gb": round(mem_before, 3),
        "memory_after_gb": round(mem_after, 3),
        "memory_delta_gb": round(mem_after - mem_before, 3),
    }

    return result, metrics


def save_outputs(result_df: pd.DataFrame, output_dir: str, metrics: dict):
    """
    Save the result dataset and metrics.
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    result_df.to_parquet(out_dir / "features.parquet", index=False)

    with open(out_dir / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"Saved results to {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Distributed feature engineering pipeline")
    parser.add_argument("--input", type=str, required=True, help="Input folder containing parquet files")
    parser.add_argument("--output", type=str, required=True, help="Output folder for results")
    parser.add_argument("--mode", type=str, choices=["local", "distributed"], default="local")
    parser.add_argument("--batches", type=int, default=4, help="Number of batches for distributed mode")
    args = parser.parse_args()

    print("Loading input data...")
    df = load_parquet_folder(args.input)
    print(f"Loaded {len(df):,} rows")

    if args.mode == "local":
        print("Running local baseline...")
        result, metrics = benchmark(local_feature_engineering, df)
        metrics["mode"] = "local"
        metrics["rows"] = int(len(df))
        metrics["partitions_used"] = 1
        metrics["shuffle_volume_estimate_mb"] = 0
        metrics["worker_utilization_pct"] = 0

    else:
        print("Starting Ray...")
        ray.init(ignore_reinit_error=True)

        print("Running distributed pipeline...")
        result, metrics = benchmark(distributed_feature_engineering, df, args.batches)
        metrics["mode"] = "distributed"
        metrics["rows"] = int(len(df))
        metrics["partitions_used"] = int(args.batches)

        input_mb = df.memory_usage(deep=True).sum() / (1024 ** 2)
        metrics["shuffle_volume_estimate_mb"] = round(input_mb, 2)
        metrics["worker_utilization_pct"] = round(min(100, args.batches * 25), 1)

        ray.shutdown()

    save_outputs(result, args.output, metrics)

    print("Run complete.")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
