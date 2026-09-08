# Multi-Machine Distributed Setup with Cloudflare Tunnel

Run a Kasoku cluster across laptops on different networks (home WiFi, office, etc.) using Cloudflare Tunnels for secure, zero-config networking.

## Prerequisites

- 2-3 machines (laptops, VMs, anything with Go installed)
- A Cloudflare account (free tier works)
- A domain managed by Cloudflare (or use `*.trycloudflare.com` quick tunnels)

---

## Step 1: Install Cloudflare Tunnel on Each Machine

### macOS
```bash
brew install cloudflared
```

### Linux (Debian/Ubuntu)
```bash
curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared
chmod +x /usr/local/bin/cloudflared
```

### Linux (Arch)
```bash
sudo pacman -S cloudflared
```

### Windows
Download from https://github.com/cloudflare/cloudflared/releases/latest and add to PATH.

---

## Step 2: Build Kasoku on Each Machine

```bash
git clone https://github.com/VanshikaSharm4/TurboDynamo.git.git
cd Kasoku
go build -o kasoku-server ./cmd/server/
go build -o ycsb-bench ./cmd/ycsb/
```

---

## Step 3: Start Cloudflare Tunnels

### Option A: Quick Tunnels (No Domain Required — Easiest)

On **each machine**, run these **before** starting Kasoku:

```bash
# Machine 1
cloudflared tunnel --url http://localhost:9100 --no-autoupdate

# Machine 2
cloudflared tunnel --url http://localhost:9100 --no-autoupdate

# Machine 3
cloudflared tunnel --url http://localhost:9100 --no-autoupdate
```

Each will output a URL like:
```
https://a1b2c3d4-localhost:9100.trycloudflare.com
```

**Write down each URL.** You'll need them for the cluster config.

### Option B: Named Tunnels (Persistent — Requires Domain)

```bash
# Login once
cloudflared tunnel login

# Create tunnels
cloudflared tunnel create kasoku-node1
cloudflared tunnel create kasoku-node2
cloudflared tunnel create kasoku-node3

# Route to localhost
cloudflared tunnel route dns kasoku-node1 kasoku1.yourdomain.com
cloudflared tunnel route dns kasoku-node2 kasoku2.yourdomain.com
cloudflared tunnel route dns kasoku-node3 kasoku3.yourdomain.com

# Start tunnels (run in background)
cloudflared tunnel run kasoku-node1 &
cloudflared tunnel run kasoku-node2 &
cloudflared tunnel run kasoku-node3 &
```

---

## Step 4: Configure Each Node

Create a config file on each machine. Replace the tunnel URLs with your actual ones.

### Machine 1 Config (`configs/multi-node1.yaml`)
```yaml
node_id: "node1"
http_port: 9001
grpc_port: 9100

storage:
  data_dir: "./data/node1"
  wal_dir: "./data/node1/wal"

cluster:
  replication_factor: 3
  w: 2
  r: 2
  seed_peers:
    - "https://kasoku1.yourdomain.com"   # or trycloudflare URL
    - "https://kasoku2.yourdomain.com"
    - "https://kasoku3.yourdomain.com"

consistency:
  vector_clock: true
  anti_entropy_interval: "30s"

failure_detection:
  phi_threshold: 8.0
  sample_size: 1000
```

### Machine 2 Config (`configs/multi-node2.yaml`)
```yaml
node_id: "node2"
http_port: 9001
grpc_port: 9100

storage:
  data_dir: "./data/node2"
  wal_dir: "./data/node2/wal"

cluster:
  replication_factor: 3
  w: 2
  r: 2
  seed_peers:
    - "https://kasoku1.yourdomain.com"
    - "https://kasoku2.yourdomain.com"
    - "https://kasoku3.yourdomain.com"

consistency:
  vector_clock: true
  anti_entropy_interval: "30s"

failure_detection:
  phi_threshold: 8.0
  sample_size: 1000
```

### Machine 3 Config (`configs/multi-node3.yaml`)
```yaml
node_id: "node3"
http_port: 9001
grpc_port: 9100

storage:
  data_dir: "./data/node3"
  wal_dir: "./data/node3/wal"

cluster:
  replication_factor: 3
  w: 2
  r: 2
  seed_peers:
    - "https://kasoku1.yourdomain.com"
    - "https://kasoku2.yourdomain.com"
    - "https://kasoku3.yourdomain.com"

consistency:
  vector_clock: true
  anti_entropy_interval: "30s"

failure_detection:
  phi_threshold: 8.0
  sample_size: 1000
```

---

## Step 5: Start the Cluster

On **each machine**, start Kasoku:

```bash
./kasoku-server --config configs/multi-node1.yaml   # Machine 1
./kasoku-server --config configs/multi-node2.yaml   # Machine 2
./kasoku-server --config configs/multi-node3.yaml   # Machine 3
```

Wait ~10 seconds for gossip to converge. Verify each node:

```bash
# On any machine or from your browser
curl https://kasoku1.yourdomain.com/health
curl https://kasoku2.yourdomain.com/health
curl https://kasoku3.yourdomain.com/health
```

Check cluster status:
```bash
curl https://kasoku1.yourdomain.com/cluster/status
```

---

## Step 6: Run Benchmarks

From **any machine** (or a 4th machine):

```bash
./ycsb-bench \
  -nodes "kasoku1.yourdomain.com:443,kasoku2.yourdomain.com:443,kasoku3.yourdomain.com:443" \
  -workers 20 \
  -batch 1 \
  -recordcount 50000 \
  -fieldlength 100 \
  -dur 10 \
  -workload b
```

> **Note:** Cloudflare Tunnels use HTTPS on port 443. The benchmark tool may need a `--tls` flag if it doesn't default to TLS. Check `./ycsb-bench --help`.

---

## Step 7: Test Fault Tolerance

### Kill a node
```bash
# On Machine 2
pkill kasoku-server

# Wait 15-30 seconds, then check from another node
curl https://kasoku1.yourdomain.com/cluster/status
```

The cluster should detect the failure via the phi accrual detector and continue serving reads/writes with the remaining 2 nodes (quorum W=2, R=2 still satisfiable).

### Restart the node
```bash
# On Machine 2
./kasoku-server --config configs/multi-node2.yaml

# The node will rejoin via gossip, receive hinted handoff data,
# and sync via Merkle anti-entropy
```

---

## Troubleshooting

### Nodes can't see each other
- Verify tunnel URLs are correct and accessible: `curl https://kasoku1.yourdomain.com/health`
- Check that `seed_peers` lists are identical on all nodes
- Look at logs: `tail -f data/node1/server.log`

### High latency over tunnels
- Cloudflare adds ~20-50ms per hop. This is expected for geographically distributed nodes.
- Reduce `anti_entropy_interval` to `60s` or `120s` to reduce background traffic.

### TLS certificate errors
- Cloudflare provides valid certs automatically. If using `trycloudflare.com` URLs, they work out of the box.
- If the benchmark tool complains about TLS, add `--insecure` or configure it to use system CA roots.

### Quick tunnel URLs change on restart
- Quick tunnels (`*.trycloudflare.com`) are ephemeral. Use named tunnels (Option B) for persistent setups.
- Or script the tunnel startup to parse and inject URLs into configs dynamically.

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Laptop 1      │     │   Laptop 2      │     │   Laptop 3      │
│   (Home WiFi)   │     │   (Office)      │     │   (Coffee Shop) │
│                 │     │                 │     │                 │
│ kasoku-server ──┼────►│ kasoku-server ──┼────►│ kasoku-server   │
│      :9100      │     │      :9100      │     │      :9100      │
│       ▲         │     │       ▲         │     │       ▲         │
│       │         │     │       │         │     │       │         │
│ cloudflared     │     │ cloudflared     │     │ cloudflared     │
└───────┼─────────┘     └───────┼─────────┘     └───────┼─────────┘
        │                       │                       │
        └───────────────────────┼───────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │   Cloudflare Edge     │
                    │   (Global Network)    │
                    └───────────────────────┘
```

All gRPC traffic between nodes is encrypted via Cloudflare's TLS. No port forwarding, no firewall rules, no public IP needed.
