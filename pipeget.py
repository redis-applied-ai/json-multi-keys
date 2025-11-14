
import os, json, time, random, argparse, sys
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
MAX_PRODUCT_ID = 6000000

def generate_random_keys(n):
    """Generate n random product keys with IDs bounded by MAX_PRODUCT_ID"""
    product_ids = random.sample(range(1, MAX_PRODUCT_ID + 1), n)
    return [f"product:{pid}" for pid in product_ids]

def test_keyset(r, keys, keyset_name):
    """Test a keyset using pipeline JSON.GET and return results"""
    print(f"\n=== Testing {keyset_name} ===")
    print(f"Attempting pipeline JSON.GET for {len(keys)} keys")
    print(f"Sample keys: {keys[:5]}{'...' if len(keys) > 5 else ''}")
    
    try:
        t0 = time.time()
        
        with r.pipeline(transaction=False) as pipe:
            for k in keys:
                # Using higher-level json().get() API within pipeline
                pipe.json().get(k, "$")
            res = pipe.execute()
            
        dt = (time.time() - t0) * 1000  # latency in milliseconds
        
        # Count successful fetches (non-None results)
        successful_fetches = sum(1 for r in res if r is not None)
        empty_results = len(keys) - successful_fetches
        
        print("Pipeline JSON.GET success.")
        print(f"Successfully fetched: {successful_fetches}/{len(keys)} keys in {dt:.2f} ms")
        
        if empty_results > 0:
            print(f"⚠️  WARNING: {empty_results} keys returned empty/null results - data may be missing from Redis")
        else:
            print(f"✅ SUCCESSFUL FETCH OF ALL KEYS")
          
        return True, dt
    except Exception as e:
        print("❌ Pipeline JSON.GET FAILED - Unable to retrieve data from Redis:")
        print(f"   Error Type: {type(e).__name__}")
        print(f"   Error Message: {e}")
        print(f"   This indicates a connection or Redis server issue")
        return False, 0

def main():
    parser = argparse.ArgumentParser(description="Test Redis pipeline JSON.GET with random product keys")
    parser.add_argument("n", type=int, help="Number of random keys to fetch")
    args = parser.parse_args()
    
    if args.n <= 0:
        print("Error: n must be a positive integer")
        sys.exit(1)
    
    if args.n > MAX_PRODUCT_ID:
        print(f"Error: n cannot exceed {MAX_PRODUCT_ID}")
        sys.exit(1)
    
    print(f"Connecting to Redis at {REDIS_URL}")
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    
    # Generate random keys
    print(f"Generating {args.n} random product keys...")
    keys = generate_random_keys(args.n)
    
    # Test the generated keyset
    success, latency = test_keyset(r, keys, f"Random Keys (n={args.n})")

if __name__ == "__main__":
    main()
