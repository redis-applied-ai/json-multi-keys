# Redis JSON Multi-key Reads

Redis JSON data loading and performance testing examples.

## Scripts

- `load.py` - Load JSON data to Redis with configurable volumes
- `mget.py` - Test JSON.MGET performance with random keys  
- `pipeget.py` - Test pipelined JSON.GET performance

## Usage

Install:
```bash
uv sync
```

Run:
```bash
# Load 6M product records
uv run python load.py -t 6000000

# Test JSON.MGET with 100 random keys -- if running in a cluster -- should see CROSS SLOT ERRORs
uv run python mget.py 100

# Test pipeline JSON.GET with 100 keys -- works!
uv run python pipeget.py 100
```

Set `REDIS_URL` environment variable to connect to remote Redis.
