# Kasoku

High-performance distributed key-value store implementing Amazon Dynamo paper with LSM-tree storage.

## Documentation

See `docs/ARCHITECTURE.md` for a complete deep dive into the system design, the LSM-tree storage engine, consistent hashing, and cluster replication mechanics.

## Quick Start

### 1. Build

```bash
go build -o kasoku-server ./cmd/server/
go build -o ycsb-bench ./cmd/ycsb/
go build -o kvctl ./cmd/kvctl/        # Redis-like CLI
cp kvctl ~/bin/                        # optional: add to PATH
export PATH="$HOME/bin:$PATH"
```

### 2. Run (Single Node)

```bash
./kasoku-server
# Server starts on http://localhost:9001, gRPC on :9100
```

With custom config:
```bash
./kasoku-server --config configs/example.yaml
```

Verify:
```bash
curl http://localhost:9001/health
```

### 3. Run (3-Node Cluster)

```bash
# Start 3 nodes (each with own config + data dir)
./kasoku-server --config configs/bench-cluster-node1.yaml &
./kasoku-server --config configs/bench-cluster-node2.yaml &
./kasoku-server --config configs/bench-cluster-node3.yaml &
sleep 3

# Verify all healthy
curl http://localhost:9001/health  # Node 1
curl http://localhost:9011/health  # Node 2
curl http://localhost:9021/health  # Node 3

# Run YCSB benchmark
./ycsb-bench \
  -nodes "localhost:9100,localhost:9101,localhost:9102" \
  -workers 50 -batch 1 -recordcount 100000 -fieldlength 100 -dur 10 -workload b
```

### Port Map

| Node | HTTP | gRPC | Config |
|------|------|------|--------|
| Node 1 | `:9001` | `:9100` | `bench-cluster-node1.yaml` |
| Node 2 | `:9011` | `:9101` | `bench-cluster-node2.yaml` |
| Node 3 | `:9021` | `:9102` | `bench-cluster-node3.yaml` |

### API Examples

```bash
# Write a key
curl -X PUT http://localhost:9001/api/v1/put/mykey -d "myvalue"

# Read a key
curl http://localhost:9001/api/v1/get/mykey

# Delete a key
curl -X DELETE http://localhost:9001/api/v1/delete/mykey

# Batch write
curl -X POST http://localhost:9001/api/v1/batch \
  -H "Content-Type: application/json" \
  -d '{"entries": {"k1": "v1", "k2": "v2"}}'

# Batch read
curl -X POST http://localhost:9001/api/v1/batch/get \
  -H "Content-Type: application/json" \
  -d '{"keys": ["k1", "k2"]}'

# Cluster status
curl http://localhost:9001/cluster/status

# Prometheus metrics
curl http://localhost:9001/metrics
```

### Full Benchmark Suite

```bash
bash benchmarks/run_ycsb_bench.sh --both --dur 10 --records 100000
```

### Docker

```bash
# Single node
docker-compose up -d

# 3-node cluster
docker-compose -f docker-compose.cluster.yml up -d
```

### CLI (Redis-like)

```bash
# Build and install
go build -o kvctl ./cmd/kvctl/
cp kvctl ~/bin/ && export PATH="$HOME/bin:$PATH"

# Remote mode (talk to running server)
kvctl --addr http://localhost:9000 put user:1 "Alice"
kvctl --addr http://localhost:9000 get user:1
kvctl --addr http://localhost:9000 delete user:1
kvctl --addr http://localhost:9000 keys
kvctl --addr http://localhost:9000 scan user:
kvctl --addr http://localhost:9000 put user:2 "Bob" -o json

# Interactive shell
kvctl --addr http://localhost:9000 shell

# Local mode (offline, opens data dir directly)
kvctl put localkey "value" --dir ./data
```

### Multi-Machine (Cloudflare Tunnel)

See `docs/MULTI-MACHINE-SETUP.md` for running across laptops on different networks.

```bash
# On each machine:
cloudflared tunnel --url http://localhost:9100
# Then use the generated .trycloudflare.com URLs as peers in config
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KASOKU_CONFIG` | Path to config file | `kasoku.yaml` |
| `KASOKU_DATA_DIR` | Override data directory | from config |
| `KASOKU_TRACING` | Enable OpenTelemetry tracing | `false` |
| `GOGC` | Go GC target percentage | `300` (auto-set) |

## Performance (YCSB-Standard Benchmarks)

These metrics represent the true, verified performance of the Kasoku Distributed LSM-Engine, measured across a 3-node cluster with strong consistency (`W=2, R=2`) and 50 concurrent workers over gRPC with tiered compaction.

### 1. Local Throughput (Pre-loading Phase)
*When writing directly to the node without network consensus routing:*
* **Local Write Throughput:** `1,000,000 to 1,700,000 keys/sec`
* **Local Write Latency:** `< 1 ms`

### 2. Distributed YCSB-Style Benchmarks
*When simulating real-world distributed workloads with `any-node-accept` routing:*

| YCSB Workload | Mix | Total Throughput | Write P99 Latency | Read P99 Latency |
| :--- | :--- | :--- | :--- | :--- |
| **Workload A** (Heavy Update) | 50% R / 50% W | **57,495 ops/sec** | 6.6 ms | 6.1 ms |
| **Workload B** (Read Mostly) | 95% R / 5% W | **70,201 ops/sec** | 5.9 ms | 5.0 ms |
| **Workload C** (Read Only) | 100% R / 0% W | **65,059 ops/sec** | N/A | 5.6 ms |
| **Max Write** (Heavy Insert) | 0% R / 100% W | **63,000 keys/sec** | 94.5 ms | N/A |

### 3. Stability & Consistency Metrics
* **Error Rate:** `0.00%` (0 dropped or failed batches across millions of operations)
* **Metadata Propagation:** Perfectly synchronized (Coordinator-Authoritative metadata ensures `Version`, `TimeStamp`, and `VectorClock` are identical across all replicas).
* **Bugs Resolved:** 50+ critical bugs fixed including WAL data loss, goroutine leaks, vector clock corruption, Merkle tree hash collisions, deadlock risks, and cache coherence violations.

### How to Reproduce

```bash
# Build the server and benchmark tool
go build -o kasoku-server ./cmd/server/
go build -o ycsb-bench ./cmd/ycsb/

# Start 3-node cluster
./kasoku-server --config configs/bench-cluster-node1.yaml &
./kasoku-server --config configs/bench-cluster-node2.yaml &
./kasoku-server --config configs/bench-cluster-node3.yaml &
sleep 3

# Run Workload B (95% Reads, 5% Writes)
./ycsb-bench \
  -nodes "localhost:9100,localhost:9101,localhost:9102" \
  -workers 50 \
  -batch 1 \
  -recordcount 100000 \
  -fieldlength 100 \
  -dur 10 \
  -workload b
```

## Project Structure

```
kasoku/
├── cmd/            # Server and CLI binaries
├── configs/        # Configuration files (single.yaml, cluster configs)
├── deploy/         # Docker, Kubernetes, monitoring
├── docs/           # All documentation
├── internal/       # Source code
├── benchmarks/     # Pressure testing tool
└── scripts/        # Benchmark scripts
```

## Key Features

- **Dynamo Paper**: Consistent hashing, quorum replication (W=2/R=2), vector clocks
- **LSM-Tree**: WAL, MemTable, SSTable, Bloom filters, compaction
- **gRPC**: High-performance RPC with connection pooling
- **Fault Tolerance**: Hinted handoff, read repair, Merkle anti-entropy
- **Production Ready**: Docker, Kubernetes, Prometheus metrics, health checks

## Distributed Tracing

Kasoku includes built-in OpenTelemetry tracing for observability across the distributed cluster.

### Enable Tracing

```bash
# Enable with stdout export (prints to console)
KASOKU_TRACING=true ./kasoku-server --config config.yaml

# Enable with OTLP export (send to Jaeger/Zipkin)
KASOKU_TRACING=true KASOKU_TRACING_EXPORTER=otlp KASOKU_OTLP_ENDPOINT=localhost:4317 ./kasoku-server --config config.yaml
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KASOKU_TRACING` | Enable tracing (`true`/`false`) | `false` |
| `KASOKU_TRACING_EXPORTER` | Export format (`stdout`, `otlp`) | `stdout` |
| `KASOKU_OTLP_ENDPOINT` | OTLP collector address | `localhost:4317` |
| `KASOKU_OTLP_INSECURE` | Use insecure gRPC | `false` |

### Traced Operations

- **gRPC Handlers**: Put, Get, BatchPut, MultiGet, Delete
- **Cluster Replication**: ReplicatedPut, ReplicatedBatchPut
- **Storage Engine**: LSM Put/Get/Batch operations
- **Cross-node**: Trace context propagates across cluster nodes

### View Traces

**Console (stdout):**
```bash
KASOKU_TRACING=true go run ./cmd/server/
# Spans print as JSON to stdout
```

**Jaeger:**
```bash
# Start Jaeger
docker run -d --name jaeger -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one

# Run server with tracing
KASOKU_TRACING=true KASOKU_TRACING_EXPORTER=otlp ./kasoku-server --config config.yaml

# Open http://localhost:16686
```

**Zipkin:**
```bash
# Start Zipkin
docker run -d --name zipkin -p 9411:9411 openzipkin/zipkin

# Run with OTLP (requires zipkin-collector)
KASOKU_TRACING=true KASOKU_TRACING_EXPORTER=otlp KASOKU_OTLP_ENDPOINT=localhost:9411 ./kasoku-server
```

### Trace Example Output

```json
{
  "Span": {
    "TraceId": "a1b2c3d4e5f6...",
    "SpanId": "1234567890ab",
    "ParentSpanId": "",
    "Name": "gRPC.BatchPut",
    "StartTime": "2026-05-03T12:00:00.000Z",
    "EndTime": "2026-05-03T12:00:00.015Z",
    "Attributes": {
      "entry_count": "100",
      "success": "true"
    }
  }
}
```

## License

Proprietary - see [docs/LICENSE](docs/LICENSE)