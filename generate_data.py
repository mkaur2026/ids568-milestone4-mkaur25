import argparse
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd


def compute_data_hash(df: pd.DataFrame) -> str:
    """
    Compute a short deterministic hash of the dataframe contents.
    """
    content = pd.util.hash_pandas_object(df, index=True).values
    return hashlib.sha256(content.tobytes()).hexdigest()[:16]


def generate_transactions(n_rows: int, seed: int = 42, skew: float = 0.2) -> pd.DataFrame:
    """
    Generate a synthetic transactions dataset.

    Parameters
    ----------
    n_rows : int
        Number of rows to generate.
    seed : int
        Random seed for reproducibility.
    skew : float
        Fraction of rows assigned to hot users to simulate skew.
    """
    rng = np.random.default_rng(seed)

    n_users = max(10000, n_rows // 100)
    user_ids = np.array([f"user_{i}" for i in range(n_users)])
    categories = np.array(["food", "travel", "shopping", "health", "bills", "entertainment"])

    hot_user_count = min(100, n_users)
    hot_users = user_ids[:hot_user_count]

    skew_mask = rng.random(n_rows) < skew
    sampled_users = rng.choice(user_ids, size=n_rows, replace=True)
    sampled_hot_users = rng.choice(hot_users, size=n_rows, replace=True)
    final_users = np.where(skew_mask, sampled_hot_users, sampled_users)

    amounts = rng.exponential(scale=50, size=n_rows).round(2)
    categories_col = rng.choice(categories, size=n_rows, replace=True)

    start_ts = np.datetime64("2025-01-01T00:00:00")
    end_ts = np.datetime64("2025-01-31T23:59:59")
    total_seconds = int((end_ts - start_ts) / np.timedelta64(1, "s"))
    offsets = rng.integers(0, total_seconds, size=n_rows)
    timestamps = start_ts + offsets.astype("timedelta64[s]")

    df = pd.DataFrame(
        {
            "user_id": final_users,
            "amount": amounts,
            "timestamp": timestamps,
            "category": categories_col,
        }
    )

    return df


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data.")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--skew", type=float, default=0.2)
    parser.add_argument("--output", type=str, default="data")
    parser.add_argument("--partitions", type=int, default=4)
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = generate_transactions(n_rows=args.rows, seed=args.seed, skew=args.skew)

    hash_value = compute_data_hash(df)

    chunks = [df.iloc[i::args.partitions] for i in range(args.partitions)]
    for i, chunk in enumerate(chunks):
        chunk_path = output_dir / f"part-{i:03d}.parquet"
        chunk.to_parquet(chunk_path, index=False)

    metadata_path = output_dir / "metadata.txt"
    with open(metadata_path, "w") as f:
        f.write(f"rows={args.rows}\n")
        f.write(f"seed={args.seed}\n")
        f.write(f"skew={args.skew}\n")
        f.write(f"partitions={args.partitions}\n")
        f.write(f"hash={hash_value}\n")

    print(f"Generated {args.rows} rows in {output_dir}")
    print(f"Verification hash: {hash_value}")


if __name__ == "__main__":
    main()
