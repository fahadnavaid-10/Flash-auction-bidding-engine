# Flash Auction High-Concurrency Bidding Engine

A high-performance bidding engine designed to handle **50,000 concurrent bidders competing in the final 10 seconds of an auction**.

Built with **FastAPI**, **Redis (Lua + Streams + Pub/Sub)**, **PostgreSQL**, **Docker Compose**, and **Next.js**.

---

## Architecture Summary

- **Atomic Hot Path:** Single-threaded Redis Lua script (`place_bid.lua`) executing bid evaluation, increment checking, stock verification, and anti-sniping dynamic extensions in $< 1\text{ ms}$.
- **Durability Tier:** Asynchronous write-ahead logging via **Redis Streams** (`XADD`) with a background worker batch-inserting records into **PostgreSQL** (`executemany`) using Consumer Groups (`XACK`).
- **Real-Time Fan-Out:** Redis Pub/Sub distribution across multiple FastAPI gateway nodes, micro-flushing batched price frames to connected WebSockets every 50ms with frame coalescing.
- **Overload Protection:** Per-user token-bucket rate limiting (10 bids/sec burst) and gateway connection ceilings.

---

## Quick Start & Experiments

### Prerequisites
- Python 3.11+
- Node.js 18+ (for Next.js frontend)
- Docker & Docker Compose (optional for containerized cluster)

Install Python dependencies:
```bash
pip install -r backend/requirements.txt
```

---

### Experiment 0: The Naive REST Race Condition Proof
Proves the fatal flaws of traditional un-synchronized read-then-write APIs under 1,000 parallel bids.

```bash
python scripts/run_naive_race_experiment.py
```
- **What it does:** Launches 1,000 parallel coroutines competing for 1 item with 1ms simulated database latency.
- **Results:** Stored in `test_runs/phase0_race_results.json`.
- **Expected Outcome:** 999 oversold items, 500+ monotonicity violations, and hundreds of duplicate prices.

---

### Experiment 1: Core Invariants Verification Suite
Validates the four non-negotiable invariants under concurrent bursts.

```bash
pytest backend/tests/test_invariants.py -v
```
- **Invariant 1:** Stock never oversold (`stock >= 0` always).
- **Invariant 2:** Price strictly increases monotonically ($B_{n+1} \ge B_n + \text{min\_increment}$).
- **Invariant 3:** Deterministic single winner under microsecond ties.
- **Invariant 4:** Idempotency (same `bid_id` retry does not bump price).

---

### Experiment 2: Redis Streams Durability & Batch Worker Test
Validates that confirmed in-memory bids are safely persisted in PostgreSQL batches with zero data loss.

```bash
pytest backend/tests/test_stream_persistence.py -v
```
- Validates `XADD` write-ahead logging.
- Validates Consumer Group reading (`XREADGROUP`) and bulk persistence.
- Validates message acknowledgment (`XACK`) and Pending Entries List (PEL) clearance.

---

### Experiment 3: Full Comparative Concurrency Benchmark
Directly pits the Naive REST Engine against the High-Concurrency Redis Lua Engine under 1,000 concurrent requests.

```bash
python scripts/benchmark_comparison.py
```
- Measures throughput (bids/sec), latency percentiles (mean, p50, p95, p99), and invariant violations.
- Saves structured comparison report in `test_runs/load_benchmark_report.json`.

---

### Experiment 4: Next.js Live Bidding Dashboard
Launches the interactive real-time auction room with WebSocket auto-reconnect, countdown clock, flashing price badge, quick-bid buttons, and anti-sniping notifications.

1. **Start the FastAPI Gateway:**
   ```bash
   uvicorn backend.app.main:app --port 8000 --reload
   ```
2. **Start the Next.js Frontend:**
   ```bash
   cd frontend
   npm install
   npm run dev
   ```
3. Open `http://localhost:3000` in multiple browser tabs to watch real-time synchronized bidding!

---

### Experiment 5: Containerized Multi-Instance Gateway behind Nginx
Spins up the full production stack: PostgreSQL, Redis, 3 FastAPI gateway instances, the Stream background worker, and the Nginx load balancer.

```bash
# Start all containers
docker compose up --build

# Run k6 stress test against Nginx load balancer (port 8000)
k6 run load_tests/k6_auction_stress.js
```

---

## Project Documentation Index

- [LEARNING.md](file:///c:/Users/KKT/Desktop/IC/LEARNING.md): Complete engineering log, design decisions, rejected alternatives, and bug fixes.
- [EXPLAINER.md](file:///c:/Users/KKT/Desktop/IC/EXPLAINER.md): Plain-language guide with real-world analogies and code line references for every concurrency concept.
- [ARCHITECTURE.md](file:///c:/Users/KKT/Desktop/IC/ARCHITECTURE.md): System architecture diagrams, step-by-step request flow, and AWS production extrapolation.
- [VIVA.md](file:///c:/Users/KKT/Desktop/IC/VIVA.md): Top 25 viva/interview questions with concise, rigorous technical answers.
