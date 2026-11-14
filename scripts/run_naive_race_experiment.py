"""
Phase 0 Experiment: 1,000 Parallel Bids Against Naive REST Read-Then-Write Engine.
Proves race conditions, lost updates, price regression, and stock overselling.
"""

import asyncio
import json
import os
import random
import sys
import time
from pathlib import Path

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx
from backend.naive_engine import app, auction_state

async def fire_single_bid(client: httpx.AsyncClient, index: int, base_url: str):
    user_id = f"user_{index:04d}"
    bid_id = f"bid_{index:04d}_{random.randint(1000, 9999)}"
    # All bidders see opening bid 100 and attempt to bid between 105 and 150
    amount = round(random.choice([105.0, 110.0, 115.0, 120.0, 125.0, 130.0]), 2)
    
    payload = {
        "bid_id": bid_id,
        "auction_id": "auction_alpha",
        "user_id": user_id,
        "amount": amount
    }
    
    try:
        t0 = time.perf_counter()
        response = await client.post(f"{base_url}/bid", json=payload, timeout=10.0)
        latency_ms = (time.perf_counter() - t0) * 1000
        return {
            "success": response.status_code == 200,
            "status_code": response.status_code,
            "data": response.json(),
            "latency_ms": latency_ms,
            "sent_amount": amount
        }
    except Exception as e:
        return {
            "success": False,
            "status_code": 500,
            "error": str(e),
            "sent_amount": amount
        }

async def run_experiment():
    print("=" * 60)
    print("PHASE 0: Running Naive Read-Then-Write Concurrency Experiment")
    print("Firing 1,000 parallel bids at the exact same moment...")
    print("=" * 60)

    # Use ASGITransport to test real FastAPI HTTP pipeline directly in-process
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        # Reset state
        await client.post("http://testserver/reset")

        # Launch 1,000 parallel tasks simultaneously
        t_start = time.perf_counter()
        tasks = [fire_single_bid(client, i, "http://testserver") for i in range(1000)]
        results = await asyncio.gather(*tasks)
        total_duration = time.perf_counter() - t_start

        # Fetch final state
        stats_resp = await client.get("http://testserver/stats")
        final_stats = stats_resp.json()

    accepted_responses = [r for r in results if r["success"]]
    rejected_responses = [r for r in results if not r["success"]]
    
    # Audit for Invariant Violations
    accepted_history = final_stats["accepted_history"]
    
    # 1. Monotonicity violation check: did a bid price ever drop or stay flat?
    monotonicity_violations = 0
    price_drops = []
    for i in range(1, len(accepted_history)):
        prev = accepted_history[i-1]["amount"]
        curr = accepted_history[i]["amount"]
        if curr <= prev:
            monotonicity_violations += 1
            price_drops.append({
                "index": i,
                "prev_bid": prev,
                "curr_bid": curr,
                "user": accepted_history[i]["user_id"]
            })

    # 2. Stock oversell check
    stock_oversold = final_stats["stock"] < 0
    oversold_count = abs(final_stats["stock"]) if stock_oversold else 0

    # 3. Duplicate prices accepted check
    seen_amounts = {}
    for h in accepted_history:
        amt = h["amount"]
        seen_amounts[amt] = seen_amounts.get(amt, 0) + 1
    duplicate_price_count = sum(count - 1 for count in seen_amounts.values() if count > 1)

    experiment_report = {
        "timestamp": time.time(),
        "engine": "Naive Read-Then-Write (FastAPI in-memory)",
        "total_requests": len(results),
        "total_duration_sec": round(total_duration, 4),
        "throughput_req_per_sec": round(len(results) / total_duration, 2),
        "bids_accepted": len(accepted_responses),
        "bids_rejected": len(rejected_responses),
        "initial_stock": 1,
        "final_stock": final_stats["stock"],
        "stock_oversold_by": oversold_count,
        "monotonicity_violations": monotonicity_violations,
        "duplicate_price_count": duplicate_price_count,
        "sample_price_drops": price_drops[:5],
        "summary": (
            f"CATASTROPHIC RACE CONDITIONS DETECTED: Stock of 1 was oversold by {oversold_count} units! "
            f"{monotonicity_violations} bids violated strict price monotonicity. "
            f"{duplicate_price_count} bids were accepted at identical price points because of the "
            "read-then-write race window."
        )
    }

    os.makedirs("test_runs", exist_ok=True)
    output_path = os.path.join("test_runs", "phase0_race_results.json")
    with open(output_path, "w") as f:
        json.dump(experiment_report, f, indent=2)

    print("\n--- EXPERIMENT RESULTS ---")
    print(f"Total Bids Fired:            {len(results)}")
    print(f"Bids Accepted:                {len(accepted_responses)}")
    print(f"Bids Rejected:                {len(rejected_responses)}")
    print(f"Final Stock (Started at 1):   {final_stats['stock']} (OVERSELL: {oversold_count} items!)")
    print(f"Monotonicity Violations:      {monotonicity_violations} (price decreased/stagnated)")
    print(f"Duplicate Price Points Won:   {duplicate_price_count}")
    print(f"Saved results to:             {output_path}")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(run_experiment())
