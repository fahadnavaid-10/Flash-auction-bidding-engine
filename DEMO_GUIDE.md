# 3-Minute Video Demo & Presentation Script (DEMO_GUIDE.md)

This guide provides the exact setup, screen layout, and word-for-word voiceover script for your **CEP Assignment Video Submission** (under 3 minutes) showcasing both the **User Bidding Room** and the **Admin & Analytics Dashboard**.

---

## 1. Screen Setup Before Recording

1. **Start the Backend Gateway** (Terminal 1):
   ```powershell
   python -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

2. **Start the Next.js Frontend** (Terminal 2):
   ```powershell
   cd frontend
   npm.cmd run dev
   ```

3. **Arrange Browser Windows (Side-by-Side Split Screen)**:
   - **Left Half of Screen:** Open `http://localhost:3000` (**User Bidding Room**).
   - **Right Half of Screen:** Open `http://localhost:3000/admin` (**Admin & Analytics Dashboard**).

4. **Recording Tool**:
   - Press **`Win + Alt + R`** (Built-in Windows Screen Recorder) with microphone enabled, or use OBS Studio.
   - Target video duration: **2 minutes 30 seconds to 2 minutes 50 seconds** (safely under the 3-minute limit).

---

## 2. Word-for-Word Voiceover Script (2:45 Runtime)

### Part 1: Problem Statement & Architectural Overview (0:00 – 0:45)

> *"Hello, my name is Fahad Navaid. In this video, I am presenting my Complex Engineering Problem (CEP) project: a high-concurrency flash-auction bidding engine engineered to handle 50,000 concurrent bidders competing in the final 10 seconds of an auction.*
>
> *Under traditional architectures, firing thousands of parallel bids at a relational database causes severe row-lock contention, connection pool starvation, and lost updates. In our initial Phase 0 test, a naive read-then-write endpoint resulted in 999 oversold items and hundreds of monotonicity violations.*
>
> *To solve this, we engineered a three-tier architecture:*
> 1. *An atomic in-memory hot path powered by single-threaded Redis Lua scripting.*
> 2. *Asynchronous durability via Redis Streams and a consumer worker batching inserts into PostgreSQL.*
> 3. *A real-time fan-out tier using FastAPI WebSockets with 50-millisecond batched frame coalescing to prevent socket saturation."*

---

### Part 2: Live Bidding in User Room & Real-Time Sync (0:45 – 1:35)

*(Action: Click the `+$5` or `+$20` button in the User Room on the left screen.)*

> *"On the left side of my screen, we have the real-time User Bidding Room connected via WebSockets. On the right side, we have our Admin & Telemetry Dashboard.*
>
> *Notice that as I place a bid of plus 5 dollars, the price immediately jumps from 100 to 105 dollars. Sub-millisecond confirmation is sent back to the user, and simultaneously, the Admin Dashboard on the right reflects the new price, current leader, and increments the total bids count.*
>
> *Every bid carries a client-generated UUID for idempotency. If a network glitch causes a client retry, the Redis Lua script recognizes the duplicate and confirms it without bumping the price twice."*

---

### Part 3: Admin Dashboard Telemetry & Concurrency Burst (1:35 – 2:15)

*(Action: Point mouse to the Admin Dashboard KPI cards, then click **'🚀 Fire 30 Concurrent Bids Burst'**.)*

> *"Now looking at the Admin Dashboard on the right, we have live telemetry:*
> - *We monitor the Redis Stream Write-Ahead Log depth, ensuring all accepted bids are queued for background PostgreSQL persistence.*
> - *We track 'Broadcast Frames Coalesced'—this is our backpressure mechanism that drops outdated price frames every 50 milliseconds so slow mobile clients do not crash gateway memory.*
> - *We see zero monotonicity violations and zero oversell.*
>
> *Now, let me simulate a sudden burst by clicking 'Fire 30 Concurrent Bids Burst'.*
> *(Click button)*
> *Immediately, 30 parallel requests hit the Redis Lua engine at once. Notice that all 30 were serialized in arrival order, price climbed monotonically to 255 dollars, the stream log captured every transaction with server timestamps, and exactly one leader emerged with zero race conditions."*

---

### Part 4: Anti-Sniping Soft-Close & Conclusion (2:15 – 2:45)

*(Action: Click **'⚡ Trigger Anti-Snipe (+30s)'** in Admin or place a bid near the deadline.)*

> *"Finally, we implemented anti-sniping soft-close rules. If a bidder tries to snipe the auction in the closing seconds, the server dynamically extends the countdown deadline by 30 seconds to allow legitimate counter-bidding—as demonstrated here by our anti-sniping trigger banner.*
>
> *In summary, our system guarantees strict price monotonicity, zero inventory oversell, deterministic single-winner selection, and high throughput under extreme contention.*
>
> *Thank you."*

---

## 3. How to Submit on Google Classroom (GCR)

1. Stop your recording (`Win + Alt + R`).
2. Your recorded video will be saved in:
   `C:\Users\KKT\Videos\Captures\`
3. Check the file properties:
   - Ensure duration is $< 3$ minutes.
   - Ensure your microphone voice audio is loud and clear.
4. Go to your **Google Classroom (GCR)** class $\to$ **Classwork** $\to$ **CEP Assignment**.
5. Click **Add or create** $\to$ **File** (or upload to Google Drive and attach link).
6. Attach:
   - Your `.mp4` video file.
   - Your GitHub repository link: `https://github.com/fahadnavaid-10/Flash-auction-bidding-engine`
7. Click **Turn In / Submit**!
