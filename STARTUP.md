# Quick Startup & Testing Guide (STARTUP.md)

Follow these simple steps to run and test the Flash Auction engine at home.

---

## 1. Environment Verification

Make sure you are in the project root directory:
```powershell
cd C:\Users\KKT\Desktop\IC
```

Ensure Python dependencies are installed:
```powershell
pip install -r backend/requirements.txt
```

---

## 2. Step 1: Run the Automated Invariant Tests (10 Seconds)

Runs the complete 9-test suite validating the 4 core concurrency invariants:
1. Stock is never oversold (`stock >= 0`).
2. Highest bid strictly increases (zero price drops).
3. Exactly one deterministic winner on microsecond ties.
4. Client idempotency (network retries do not bump price twice).

```powershell
python -m pytest backend/tests -v
```

---

## 3. Step 2: Run the "Before vs. After" Concurrency Benchmark

Runs 1,000 parallel virtual bidders simultaneously against both the **Naive REST Engine** and the **High-Concurrency Redis Lua Engine**:

```powershell
python scripts/benchmark_comparison.py
```

### What You Will See in the Output:
- **Naive REST:** Shows **999 oversold items** (stock drops to `-999`), and **~590 monotonicity violations** where price dropped or stagnated.
- **Redis Lua Engine:** Shows **0 oversold items**, **0 monotonicity violations**, and **1 unambiguous winner**.
- Results are saved to `test_runs/load_benchmark_report.json`.

---

## 4. Step 3: Start the Backend Gateway Server

Starts the live FastAPI WebSocket and REST server on port `8000`:

```powershell
python -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Verify It's Running:
- **Interactive API Docs:** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Check Auction State:** [http://localhost:8000/auction/auction_alpha](http://localhost:8000/auction/auction_alpha)

### Send a Test Bid from PowerShell:
```powershell
curl.exe -X POST "http://localhost:8000/bid" `
  -H "Content-Type: application/json" `
  -d '{"bid_id": "test_1", "auction_id": "auction_alpha", "user_id": "alice", "amount": 105.0}'
```

---

## 5. Step 4: Start the Next.js Real-Time Bidding Frontend

Open a **new, separate terminal window**:

```powershell
cd C:\Users\KKT\Desktop\IC\frontend
npm install
npm run dev
```

1. Open [http://localhost:3000](http://localhost:3000) in your web browser.
2. Open a **second browser window** (or incognito) to [http://localhost:3000](http://localhost:3000).
3. Click the **"+$5"** or **"+$20"** bid buttons in one window:
   - Notice how both windows update in real time via the 50ms WebSocket broadcast loop.
   - Watch the countdown timer, flashing price badge, and the indicator showing who holds the winning bid.
   - If you bid in the final 5 seconds, watch the **anti-sniping banner** extend the deadline by 10 seconds!

---

## 6. Step 5 (Optional): Run the Full Docker Compose Production Cluster

If you have **Docker Desktop** installed and running on your machine:

```powershell
# Build and start PostgreSQL, Redis, 3 Gateway instances, and Nginx:
docker compose up --build
```

- Nginx will load-balance traffic at [http://localhost:8000](http://localhost:8000).
- If you have **k6** installed, run the stress test against the cluster:
  ```powershell
  k6 run load_tests/k6_auction_stress.js
  ```

---

## Documentation Quick Links

- [LEARNING.md](file:///c:/Users/KKT/Desktop/IC/LEARNING.md): Architectural decisions and rejected alternatives.
- [EXPLAINER.md](file:///c:/Users/KKT/Desktop/IC/EXPLAINER.md): Plain-language guide with real-world analogies and code line references.
- [ARCHITECTURE.md](file:///c:/Users/KKT/Desktop/IC/ARCHITECTURE.md): System architecture diagrams and request flows.
- [VIVA.md](file:///c:/Users/KKT/Desktop/IC/VIVA.md): 25 interview/viva questions with concise technical answers.
