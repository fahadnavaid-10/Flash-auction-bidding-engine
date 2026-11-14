# Engineering Log & Learning Journal: Flash Auction Engine

## 1. Project Overview & Challenge
- **Objective:** Build a high-concurrency flash-auction bidding engine handling 50,000 concurrent bidders in the final 10 seconds of an auction.
- **Key Challenges:** Extreme lock contention, write throughput, sub-100ms real-time fan-out, memory vs. disk durability trade-offs, and horizontal scalability.

---

## 2. Core Invariants (Non-Negotiable System Guarantees)

1. **Monotonic Price Progression:**
   The highest bid price for an auction strictly increases ($B_{n+1} > B_n$). A bid lower than or equal to the current highest bid is rejected.
2. **Strict Increments:**
   Every valid bid must satisfy:
   $$B_{\text{new}} \ge B_{\text{current}} + \text{min\_increment}$$
3. **Server-Side Timing Authority:**
   All bid timestamps and auction deadlines are evaluated exclusively using the server-side clock (Redis `TIME` / system clock). Client timestamps are discarded. No bids are accepted after `auction_end_time`.
4. **Stock Safety (No Oversell):**
   Exactly one unit of inventory is auctioned per listing. The item cannot be allocated more than once under any race condition.
5. **Deterministic Winner Selection:**
   If multiple bidders submit bids at virtually the same instant, the engine processes them sequentially. Exactly one bidder is declared the winner based on strict atomic arrival order. No shared or ambiguous winners.
6. **Per-Auction Strict Ordering:**
   All state mutations for a given auction are strictly serialized. Bids for Auction A never block or interleave with Auction B, but all bids for Auction A execute in total arrival order.
7. **Idempotency (Client Bid ID):**
   Every bid submission carries a unique client-generated `bid_id` (UUID). Network retries or rapid double-clicks with the same `bid_id` are recognized as duplicates and do not increment the price twice.
8. **Durability & The Redis-Crash Durability Gap:**
   Every accepted bid is buffered in an append-only stream and persisted to PostgreSQL.
   - **The Durability Gap Note:** Because bids are accepted in Redis memory first for speed, a hard server crash before PostgreSQL persistence creates a vulnerability window.
   - **The AOF Trade-off:**
     - `appendfsync everysec`: High throughput (~50k-100k ops/sec), but a catastrophic crash can lose up to 1 second of acknowledged bids.
     - `appendfsync always`: Guaranteed zero-loss persistence on Redis crash, but forces disk I/O on every single bid, dropping throughput to ~1,000-2,000 ops/sec and defeating in-memory performance.
     - *Status:* We will formally test and decide this trade-off in **Phase 2**.

---

## 3. Phased Roadmap & Design Decisions

### Phase 0: Naive REST Read-Then-Write Baseline & Race Condition Proof
- **Problem Solved:** Establish a control baseline. Prove empirically why standard database or in-memory read-modify-write patterns completely fail under concurrency.
- **Deliverables:**
  - Minimal FastAPI endpoint with naive "check highest bid -> update highest bid" logic.
  - Stress script firing 1,000 parallel bids.
  - Recorded proof of lost updates, duplicate winners, and negative increments saved in `test_runs/phase0_race_results.json`.
- **Rejected Alternatives:**
  1. *Optimistic Concurrency Control (OCC) with DB Version Columns:*
     - *Why Rejected:* Under 50,000 concurrent requests contending for a single row, OCC causes a 99.9% transaction abort rate. Retry storms amplify load and saturate database connections without making forward progress.
  2. *Client-side Throttling / Queuing:*
     - *Why Rejected:* Untrusted clients can bypass frontend throttles with raw HTTP requests. Network latency variance also means client queue ordering does not reflect real arrival order.

---

### Phase 1: The Atomic Hot Path (Redis + Lua Scripting)
- **Problem Solved:** Eliminate database row locks and application-tier race conditions by evaluating bid eligibility, increment checks, idempotency, and price updates in a single atomic in-memory operation.
- **Deliverables:**
  - `place_bid.lua` script running in single-threaded Redis.
  - Python wrapper and integration tests verifying strict monotonicity and zero race conditions under 1,000 parallel workers.
- **Rejected Alternatives:**
  1. *Distributed Locking (Redlock / Redis Mutex):*
     - *Why Rejected:* Acquiring and releasing a distributed lock requires multiple network round-trips per bid ($O(N)$ network hops). 50,000 requests contending for one lock produces massive queueing delays (>500ms latency). Lua runs locally on the Redis engine in microseconds.
  2. *PostgreSQL Pessimistic Locking (`SELECT ... FOR UPDATE`):*
     - *Why Rejected:* Holding a database row lock during disk writes causes connection pool exhaustion, extreme lock wait queues, and thread starvation under thousands of requests per second.

---

### Phase 2: Asynchronous Durability (Redis Streams → Worker → PostgreSQL)
- **Problem Solved:** Decouple the low-latency bid acceptance path from slow relational disk writes while guaranteeing eventual persistence and crash recovery.
- **The Redis-Crash Durability Gap & AOF Decision Analysis:**
  - *The Architectural Reality:* In a flash auction with 50,000 concurrent bidders, synchronous disk writes per bid are physically impossible (disk fsync latency ~1-10ms per I/O vs. required in-memory latency <1ms). Bids are therefore acknowledged to clients once committed to the Redis append-only stream in RAM.
  - *The Gap:* If the Redis master process crashes or experiences power failure before the background worker drains the stream to PostgreSQL, recently acknowledged bids in memory could be lost.
  - *Evaluating Redis Persistence Modes:*
    1. **`appendfsync always`:** Flushes every write command to disk before returning.
       - *Pros:* Zero data loss on hard crash.
       - *Cons:* Write throughput drops to ~1,200 ops/sec. Under 50,000 bidders, requests queue, latency balloons to hundreds of milliseconds, and the auction engine freezes.
    2. **`appendfsync everysec` (CHOSEN):** Redis background-threads `fsync` once per second.
       - *Pros:* Sustains 50,000 - 100,000+ ops/sec throughput at sub-millisecond latency.
       - *Cons:* In the catastrophic event of a hard OS kernel panic or host power loss, up to 1 second of confirmed bids could be lost.
    3. **`appendfsync no`:** Lets the OS flush buffers whenever it wants (30+ seconds). Rejected due to unacceptable data loss window.
  - *Decision & Mitigation:* We configure Redis with `appendfsync everysec`. In enterprise production on AWS, this is paired with Redis ElastiCache Multi-AZ replication with automatic failover (replicating in-memory buffers across availability zones in <2ms) and PostgreSQL as the authoritative long-term system of record.
  - *Consumer Group Checkpointing:* The worker utilizes Redis Consumer Groups (`XREADGROUP`) and explicit acknowledgments (`XACK`). If the worker crashes mid-batch, unacknowledged bids remain in the Pending Entries List (PEL) and are safely re-claimed by another worker instance without data loss.
- **Deliverables:**
  - `backend/app/worker.py`: Consumer group stream batch processor.
  - `backend/app/db.py`: Relational schema and batch execution (`executemany`).
  - `backend/tests/test_stream_persistence.py`: End-to-end stream durability test.
- **Rejected Alternatives:**
  1. *Synchronous Dual-Write (Write Redis + Write Postgres in the same request):*
     - *Why Rejected:* The latency of the entire request is bound to the slowest component (Postgres disk I/O). If Postgres slows down or deadlocks, the hot bidding path halts.
  2. *In-Memory Python Queue (`asyncio.Queue`):*
     - *Why Rejected:* If the FastAPI process restarts or crashes, all unwritten bids in memory are permanently lost. It also cannot be consumed by separate worker processes across multiple gateway nodes.

---

### Phase 3: Auction Rules, Anti-Sniping & Server Timestamps
- **Problem Solved:** Enforce hard auction closing rules, prevent client clock tampering, and implement anti-sniping (soft close: extending the auction by $X$ seconds if a bid arrives in the final $Y$ seconds).
- **Deliverables:**
  - Server-side time validation inside the Lua script (`redis.call('TIME')`).
  - Dynamic `end_time` extension logic (anti-sniping trigger).
  - Tests validating bids rejected after deadline and deadline extensions under last-second bidding.
- **Rejected Alternatives:**
  1. *Client-Side Timestamps:*
     - *Why Rejected:* Client clocks drift by seconds or minutes and can be trivially manipulated by malicious users to backdate bids.
  2. *Application-Tier Sleep / Timers (`asyncio.sleep`):*
     - *Why Rejected:* In-process timers in Python are lost if the server process restarts and do not synchronize across multiple gateway instances.

---

### Phase 4: Multi-Instance Gateway & Real-Time Fan-Out (Nginx + FastAPI + Redis Pub/Sub)
- **Problem Solved:** Scale connection capacity horizontally across multiple CPU cores/processes to handle 50,000 WebSocket connections, and broadcast new price points in sub-100ms.
- **Deliverables:**
  - Nginx reverse proxy load-balancing across multiple FastAPI gateway instances.
  - Redis Pub/Sub distribution: single Lua publish fans out to all gateway workers, which broadcast to their local WebSocket clients.
  - Connection lifecycle and heartbeat manager.
- **Rejected Alternatives:**
  1. *Single Monolithic Gateway Process:*
     - *Why Rejected:* A single Python event loop is CPU-bound by JSON serialization and TLS/socket overhead when handling tens of thousands of active WebSockets, capping throughput around 10k connections.
  2. *Client HTTP Polling / SSE:*
     - *Why Rejected:* 50,000 clients polling every 500ms creates 100,000 HTTP requests/second, overwhelming the web server with header parsing and connection handshakes.

---

### Phase 5: Overload Protection & Traffic Shaping (Rate Limits, Caps, Backpressure)
- **Problem Solved:** Prevent malicious bidders or bot scripts from starving legitimate users, protect gateway memory from slow consumers, and maintain sub-100ms responsiveness under extreme spikes.
- **Deliverables:**
  - Per-user token bucket rate limiter in Redis (e.g., max 5 bids/sec per user).
  - Gateway WebSocket connection ceiling per IP.
  - Backpressure handling: drop or coalesce outdated price broadcast frames for slow WebSocket clients to avoid server memory bloat.
- **Rejected Alternatives:**
  1. *Unbounded In-Memory Broadcast Queues:*
     - *Why Rejected:* If a client on a slow cellular connection lags, buffering every price update in server memory leads to Out-Of-Memory (OOM) crashes across gateway instances.
  2. *Global Coarse-Grained Rate Limiter:*
     - *Why Rejected:* A global limit drops traffic indiscriminately, allowing a few aggressive bots to consume the entire capacity and lock out genuine bidders.

---

### Phase 6: Load Testing, Invariant Verification & Benchmarking (k6 / Locust)
- **Problem Solved:** Empirically stress-test the end-to-end engine under simulated flash-auction traffic (1,000 to 5,000+ virtual users), measure p95/p99 latency, and document laptop physical limits.
- **Deliverables:**
  - k6 / Locust scenarios simulating the final 10-second surge.
  - Invariant assertion suite: verifying zero oversells, strict price monotonicity, idempotency under duplicate bursts, and database parity.
  - Performance profile report (throughput, latency percentiles, CPU/memory usage).
- **Rejected Alternatives:**
  1. *Synthetic Single-Threaded Benchmark (Sequential `curl` loop):*
     - *Why Rejected:* Sequential benchmarks cannot reproduce lock contention, socket queueing delays, or race conditions.
  2. *Running Client Generators and Servers Without Resource Caps:*
     - *Why Rejected:* The load generator starves the server for CPU and ephemeral ports on the same machine, leading to false-negative latency spikes.

---

### Phase 7: Real-Time Client Dashboard (Next.js)
- **Problem Solved:** Provide an intuitive, low-latency bidding interface with real-time countdown, live price updates, connection health indicators, and optimistic UI with server reconciliation.
- **Deliverables:**
  - Next.js WebSocket client component.
  - Optimistic bid status (Pending -> Confirmed -> Outbid / Won).
  - Graceful reconnection and state synchronization on disconnect.
- **Rejected Alternatives:**
  1. *Pure Optimistic UI Without Rollback:*
     - *Why Rejected:* Shows "You are the highest bidder!" even if the atomic hot path rejected the bid, leading to user confusion and disputed auction outcomes.
  2. *Polled Revalidation (`useSWR` / Polling):*
     - *Why Rejected:* Polling intervals (1-2s) are far too coarse for a 10-second flash auction where price changes multiple times per second.

---

## 4. Benchmark & Experiment Results

### Phase 0: Naive REST Read-Then-Write Baseline Experiment
- **Setup:** 1,000 parallel coroutines firing simultaneous bids between $105 and $130 against initial price $100 and stock of 1 item with 1ms simulated I/O delay.
- **Results (`test_runs/phase0_race_results.json`):**
  - **Bids Sent:** 1,000
  - **Bids Accepted:** 1,000 (100% false acceptance!)
  - **Stock Remaining:** -999 (Oversold by 999 units!)
  - **Monotonicity Violations:** 571 times the accepted price stayed flat or dropped backwards.
  - **Duplicate Prices Awarded:** 994 bids awarded at identical price points.
- **Root Cause Analysis:** Classic Time-Of-Check to Time-Of-Use (TOCTOU). When the async handler awaits I/O (`await asyncio.sleep(0.001)`), control returns to the Python event loop. All 1,000 requests read the identical initial price ($100) and stock (1), pass the validation check concurrently, and sequentially execute un-synchronized blind writes.

---

### Phase 9: Comparative Concurrency Benchmark (Naive vs High-Concurrency Engine)
- **Report Location:** `test_runs/load_benchmark_report.json`
- **Workload:** 1,000 parallel virtual bidders fired concurrently against both engines.
- **Comparative Metrics Table:**

| Metric | Naive REST Engine | Redis Lua High-Concurrency Engine |
|---|---|---|
| **Stock Oversold Count** | **999 items** (Catastrophic oversell) | **0 items** (Absolute safety) |
| **Monotonicity Violations** | **593 violations** (Price went backward) | **0 violations** (Strictly monotonic) |
| **Accepted High Bid** | $110.0 (Stagnated due to lost updates) | $607.31 (Real market price discovery) |
| **Throughput** | 2,528.5 req/sec (Un-synchronized false speed) | 587.4 req/sec (In-process Lua under Python GIL) |
| **p95 Latency** | 267.5 ms | 1,522.5 ms (Python in-process Lupa emulation) |
| **Single Deterministic Winner** | FAILED (Hundreds believed they won) | PASSED (Exactly 1 winner) |

---

### Hardware Limits: What a Single Laptop Cannot Prove
1. **Loopback & Ephemeral Port Exhaustion:**
   On a single OS, running 50,000 active TCP client connections to loopback (`127.0.0.1`) exhausts ephemeral ports (default range `~49152 - 65535`, giving only ~16k ports per IP) and triggers TCP `TIME_WAIT` socket starvation.
2. **Resource Starvation Between Load Generator and Target:**
   Running virtual users and servers on the same CPU cores means the load generator steals CPU cycles, L1/L2 cache, and memory bandwidth from the auction engine, inflating latency metrics.
3. **True Network Jitter & Geographic Delay:**
   Real-world 50,000 bidders connect over 4G/5G, broadband, and mobile networks with 20ms to 200ms latency and packet drop rates.

---

### Production Capacity Extrapolation (AWS Deployment Architecture)
To handle **50,000 concurrent bidders competing in the final 10 seconds** in production:
- **Load Balancer:** AWS Network Load Balancer (NLB) operating at Layer 4 (TCP), distributing incoming TLS connections across gateway nodes with zero HTTP termination bottlenecks.
- **WebSocket Gateway Tier:** 4x Amazon ECS Fargate tasks (4 vCPU, 8GB RAM each) running FastAPI / Uvicorn with `uvloop`.
  - Max capacity per node: 12,500 - 15,000 WebSocket connections.
  - Total fleet capacity: 50,000 - 60,000 persistent connections.
- **Hot Path In-Memory Engine:** AWS ElastiCache for Redis (m6g.2xlarge with Graviton3 processors, 8 vCPU, 26GB RAM).
  - Native C compiled Redis runs single-threaded commands at **100,000+ ops/second** at `< 0.8ms` p99 latency.
- **Persistence Tier:** Amazon Aurora PostgreSQL Multi-AZ (db.r6g.xlarge). Background stream consumer workers pool writes via `executemany` batches of 500 records every 50ms, consuming only ~20-50 DB connections.

## 5. Mistakes, Bugs & Fixes

1. **Redis Connection Pool Exhaustion under Async Burst (`MaxConnectionsError`):**
   - *Symptom:* When firing 200 concurrent coroutines in `test_lua_concurrent_race_resistance`, Redis threw `MaxConnectionsError: Too many connections`.
   - *Root Cause:* Default `aioredis` / `fakeredis` connection pool was capped at 50 connections. 200 coroutines attempted to acquire simultaneous client handles.
   - *Fix:* Configured `max_connections=1000` in `RedisEngine` connection pool and ensured proper connection recycling. In high-concurrency production, connection pool size must be tuned to match the expected worker concurrency or throttled through a bounded asyncio semaphore.

---
