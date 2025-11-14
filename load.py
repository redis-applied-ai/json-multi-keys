
import os, json, sys, time, argparse
from pathlib import Path
import redis
from redis.commands.json.path import Path as JsonPath

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATASET = os.getenv("DATASET_PATH", "dataset.jsonl")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))


def parse_args():
    parser = argparse.ArgumentParser(description="Load data to Redis with configurable total amount")
    parser.add_argument("--total", "-t", type=int, default=5000,
                       help="Total amount of data to write (default: 5000)")
    return parser.parse_args()


def load_base_dataset(dataset_path):
    """Load the base dataset into memory for cycling through"""
    base_data = []
    with open(dataset_path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line.strip())
            base_data.append(obj["value"])  # We only need the value part
    return base_data


def main():
    args = parse_args()
    
    print(f"Connecting to Redis at {REDIS_URL} ...")
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    
    # Test connection
    try:
        r.ping()
        print("✓ Redis connection successful")
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        sys.exit(1)
    
    path = Path(DATASET)
    if not path.exists():
        print(f"✗ Dataset not found at {path}")
        sys.exit(1)
    
    print(f"✓ Found dataset at {path}")
    print(f"✓ Using batch size: {BATCH_SIZE}")
    print(f"✓ Target total records: {args.total}")
    
    # Load base dataset into memory
    print("Loading base dataset...")
    base_data = load_base_dataset(path)
    base_size = len(base_data)
    print(f"✓ Loaded {base_size} base records")
    print("Starting data load...")

    t0 = time.time()
    total = 0
    batch_count = 0
    skipped = 0

    with r.pipeline(transaction=False) as pipe:
        for i in range(args.total):
            # Cycle through base data using modulo
            base_record = base_data[i % base_size]
            
            # Create new record with sequential ID
            new_record = base_record.copy()
            new_record["id"] = i + 1  # Start IDs from 1
            
            # Create key using the new ID
            key = f"product:{i + 1}"
            
            pipe.json().set(key, JsonPath.root_path(), new_record)
            total += 1
            
            if (i + 1) % BATCH_SIZE == 0:
                batch_count += 1
                pipe.execute()
                print(f"✓ Batch {batch_count}: {BATCH_SIZE} records | Total: {total}")
                        
        # Execute remaining records in pipeline
        if total % BATCH_SIZE != 0:
            batch_count += 1
            remaining = total % BATCH_SIZE
            pipe.execute()
            print(f"✓ Final batch {batch_count}: {remaining} records | Total: {total}")

    dt = time.time() - t0
    avg_rate = total / dt if dt > 0 else 0
    
    print(f"\n🎉 Success! Loaded {total} JSON documents in {dt:.2f}s")
    if skipped > 0:
        print(f"⚠ Skipped {skipped} invalid records")
    print(f"📊 Average rate: {avg_rate:.1f} docs/sec across {batch_count} batches")
    print(f"📈 Dataset cycles: {(args.total - 1) // base_size + 1} (base dataset size: {base_size})")

if __name__ == "__main__":
    main()
