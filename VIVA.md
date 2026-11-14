# High-Concurrency Flash Auction: Top 25 Viva Questions & Technical Answers (VIVA.md)

---

### Q1: Why not use simple REST polling from the frontend every 500ms?
**Answer:** At 50,000 concurrent bidders, polling every 500ms results in **100,000 HTTP requests per second** hitting the web tier continuously. 99% of those requests return unchanged data, wasting CPU on TLS handshakes, HTTP header parsing, and socket allocation. WebSockets maintain a single persistent TCP connection per client, reducing server overhead to zero except when a new price event is pushed.

---

### Q2: Why use a Lua script inside Redis instead of a standard `GET` followed by `SET` in Python?
**Answer:** `GET` followed by `SET` creates a **Time-Of-Check to Time-Of-Use (TOCTOU)** race condition. Between the `GET` returning to Python and the subsequent `SET` reaching Redis, dozens of other parallel requests can execute their own writes. Redis executes Lua scripts **atomically within its single-threaded execution thread**; no other command or script can interleave during its execution.

---

### Q3: What happens if the Redis server crashes mid-auction?
**Answer:** Because we use `appendfsync everysec`, a catastrophic power cut can lose at most the final 1 second of confirmed bids not yet flushed to disk. On reboot, Redis reconstructs state from its AOF file. If Redis fails completely, PostgreSQL holds all previously acknowledged batches; we can rebuild the active auction state by querying `SELECT MAX(amount) FROM bids WHERE auction_id = ?` and repopulating Redis cache. In production AWS environments, this risk is mitigated via Multi-AZ ElastiCache replication with automatic failover in <2 seconds.

---

### Q4: Explain the Redis AOF persistence trade-off: `appendfsync always` vs `appendfsync everysec`.
**Answer:** 
- `appendfsync always`: Calls `fsync()` on every command before returning. Guarantees zero data loss, but throughput collapses to ~1,200 ops/sec due to disk seek latency, causing the entire bidding engine to choke.
- `appendfsync everysec`: Buffers writes in memory and flushes to disk in a background thread once per second. Achieves **80,000+ ops/sec** at sub-millisecond latency, trading a theoretical maximum 1-second loss window for a 70x throughput improvement.

---

### Q5: Why use Redis Streams instead of an in-process `asyncio.Queue` in FastAPI for persistence?
**Answer:** An in-process `asyncio.Queue` lives in the memory space of a single Python gateway process. If that gateway process restarts, deploys, or crashes, all queued bids are permanently lost. Furthermore, multiple gateway instances cannot share an in-memory queue. Redis Streams provides a centralized, durable, multi-process write-ahead buffer accessible across the entire gateway cluster.

---

### Q6: Why not use Redis Pub/Sub directly to feed the database consumer worker?
**Answer:** Redis Pub/Sub is **fire-and-forget**. It provides zero message buffering, zero delivery guarantees, and no persistence. If the database worker restarts or slows down, messages sent to Pub/Sub during that downtime are permanently lost. Redis Streams provides persistent storage, offsets, Consumer Groups, and Pending Entries Lists (PEL).

---

### Q7: What is a Consumer Group and Pending Entries List (PEL) in Redis Streams?
**Answer:** A Consumer Group distributes stream messages across multiple competing workers. When a worker reads a message via `XREADGROUP`, Redis marks it as delivered and places it into the **Pending Entries List (PEL)**. The message remains in the PEL until the worker successfully writes it to PostgreSQL and calls `XACK`. If the worker dies before calling `XACK`, another worker can inspect the PEL (`XPENDING` / `XCLAIM`) and safely finish persisting the message.

---

### Q8: Why rejected: PostgreSQL row-level locking (`SELECT ... FOR UPDATE`)?
**Answer:** When 5,000 concurrent requests try to lock the same auction row in PostgreSQL within 1 second, they form a massive queue waiting for the row lock. Database connection pools (max 50-100 connections) are immediately exhausted, transaction latencies skyrocket past 500ms, and PostgreSQL deadlocks or drops connections.

---

### Q9: Why rejected: Optimistic Concurrency Control (OCC) using a version column in PostgreSQL?
**Answer:** OCC works well when contention is low. Under 50,000 concurrent bidders competing for a single item, **99.9% of transactions abort due to version mismatch**. Retrying aborted transactions creates a catastrophic retry storm that amplifies system load while accomplishing virtually no work.

---

### Q10: How is strict price monotonicity ($B_{n+1} \ge B_n + \text{min\_increment}$) guaranteed?
**Answer:** The Lua script reads the current highest bid from the Redis Hash and compares it against the proposed amount in the same atomic block. If the proposed amount is lower than `current_highest + min_increment`, the script rejects the bid immediately. Because Redis executes scripts sequentially, no two bids can ever pass this check on stale state.

---

### Q11: How does the engine prevent stock overselling (stock going negative)?
**Answer:** The Lua script checks `stock > 0` before accepting any bid. For an auction with 1 item, stock is maintained at 1 until close or decremented to 0 upon winning allocation. Any bid arriving when stock is 0 is rejected with `OUT_OF_STOCK` inside the atomic block.

---

### Q12: How does the engine resolve deterministic winners when multiple bids arrive at the exact same sub-millisecond?
**Answer:** Redis processes commands sequentially on a single thread. Whichever network packet reaches the Redis socket buffer first is executed first by the Lua engine. That bid is accepted as the new high price; all other bids arriving in subsequent microsecond turns with the same or lower amount fail the minimum increment check and are rejected.

---

### Q13: How does idempotency protect against network retries and double-clicking?
**Answer:** Every bid submission carries a client-generated UUID (`bid_id`). The Lua script checks `HEXISTS auction:{id}:bids_seen bid_id`. If the ID was already recorded, the script immediately returns `IDEMPOTENT_DUPLICATE` along with the previously confirmed amount without re-raising the price or writing a second stream entry.

---

### Q14: Why do we batch WebSocket broadcast fan-outs every 50-100ms instead of broadcasting immediately on every accepted bid?
**Answer:** If 1,000 bids are accepted per second and 50,000 clients are connected, instant broadcasting requires **50,000,000 socket write operations per second**. This saturates the CPU event loops of the gateway servers and fills OS TCP socket buffers, causing memory leaks and dropped connections. Batching flushes only the latest price frame 10–20 times per second, which perfectly matches human visual perception (60Hz) while reducing socket write overhead by 95%.

---

### Q15: What is Frame Coalescing, and how does it prevent server Out-Of-Memory (OOM)?
**Answer:** If five price updates occur within a single 50ms window, Frame Coalescing overwrites the pending broadcast buffer with the newest update instead of queueing all five. Intermediate price transitions are dropped because clients only require the current highest bid. This prevents unbound queue growth in gateway memory when clients or sockets lag.

---

### Q16: How does anti-sniping (soft close) work, and why must it use server-side timestamps?
**Answer:** Anti-sniping detects if an accepted bid arrives within the final $N$ seconds (e.g., 5s) of the auction. If triggered, it dynamically extends `end_time` by $M$ seconds (e.g., 10s). It must use Redis server time (`redis.call('TIME')`) because client clocks drift by seconds or can be intentionally altered by malicious bidders to simulate early arrival.

---

### Q17: What is the difference between `EVAL` and `EVALSHA` in Redis?
**Answer:** `EVAL` requires sending the entire Lua script text over the network on every invocation, consuming significant network bandwidth. `EVALSHA` pre-loads the script into Redis memory via `SCRIPT LOAD` and invokes it using its 40-character SHA1 hash digest, reducing network payload from kilobytes to 40 bytes.

---

### Q18: What is connection pool exhaustion, and what caused it in our test suite?
**Answer:** Connection pool exhaustion occurs when asynchronous worker tasks attempt to acquire more simultaneous client connections than the pool's configured ceiling (e.g., 200 coroutines competing for a pool of 50 connections). Tasks block and eventually throw `MaxConnectionsError`. We resolved it by sizing the connection pool to 1,000 connections and implementing bounded concurrency.

---

### Q19: Why do we need Nginx or an AWS Network Load Balancer (NLB) in front of FastAPI?
**Answer:** A single Python process runs on one CPU core (constrained by the Python GIL for CPU tasks). To handle 50,000 concurrent persistent WebSocket connections, we deploy multiple gateway instances across CPU cores. Nginx (or an AWS Layer 4 NLB) terminates incoming TCP traffic and distributes WebSocket connections across the gateway cluster.

---

### Q20: Why choose Layer 4 (TCP) over Layer 7 (HTTP) for high-scale WebSocket load balancing?
**Answer:** Layer 7 proxies parse and buffer HTTP/WebSocket framing at the application layer, requiring substantial CPU and memory per connection. Layer 4 proxies (such as AWS NLB) operate at the transport layer, routing raw TCP packets directly without protocol inspection, resulting in ultra-low latency and supporting millions of concurrent persistent connections per load balancer.

---

### Q21: How does token bucket rate limiting protect the hot path?
**Answer:** Each user is assigned a token bucket (e.g., 10 tokens, refill rate 5 tokens/second). Each bid consumes 1 token. If a bot script fires 100 bids/second, the bucket drains immediately and subsequent requests are rejected at the gateway with HTTP 429 before they ever touch Redis or the Lua script.

---

### Q22: What happens when an auction reaches `end_time` while bids are in flight?
**Answer:** When the Lua script runs for an incoming bid, it reads Redis server time. If `server_time > end_time`, it marks the auction status as `CLOSED` in Redis and rejects the bid with `AUCTION_CLOSED`. Even if a bid was dispatched over the network 1 millisecond before the deadline, only bids that execute *before* the server clock crosses `end_time` are valid.

---

### Q23: Why can't a single laptop fully prove 50,000 concurrent WebSocket connections?
**Answer:** A single operating system has an ephemeral port range limit (~16,384 to 65,535, giving ~50,000 ports total shared across all processes). When both the load generator and the server run on `127.0.0.1`, running 50,000 client sockets plus server sockets exhausts local ports and file descriptors (`ulimit -n`). Furthermore, the load generator competes with the server for CPU cache, memory bus bandwidth, and CPU cores.

---

### Q24: How would you scale this architecture horizontally to 500,000 bidders across multiple regions?
**Answer:**
1. **Edge:** AWS Route 53 latency-based routing to the nearest AWS Region.
2. **Single Global Master for Write Path:** Because an auction has one unambiguous winner, the hot-path Redis Lua engine must remain in a single designated region (e.g., `us-east-1`) to avoid distributed cross-region write consensus latency.
3. **Regional Edge Gateways:** Edge regions run WebSocket gateway clusters that ingest client connections and forward bids over AWS Direct Connect / backbone VPC peering to the primary Redis cluster.
4. **Edge Read Replicas:** Price updates are published globally via Redis Streams / Kafka to edge gateways for local sub-50ms fan-out.

---

### Q25: How do you reconcile discrepancies between Redis and PostgreSQL after an unexpected disaster recovery?
**Answer:** A reconciliation script inspects the maximum bid recorded in the PostgreSQL `bids` audit table for the active auction. If Redis lost memory state, the script queries `SELECT amount, user_id FROM bids WHERE auction_id = ? ORDER BY amount DESC LIMIT 1` and executes a Redis `HSET` to seed the exact recovered price and winner into Redis before reopening the auction to traffic.
