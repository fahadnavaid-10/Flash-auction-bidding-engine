"""
Naive REST /bid implementation (Read-Then-Write).
Demonstrates the classic Check-Then-Act / Time-Of-Check to Time-Of-Use (TOCTOU) race condition.
"""

import asyncio
import time
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Naive Auction Engine (Baseline)")

# In-memory shared state simulating an un-synchronized database row or cache
auction_state = {
    "auction_id": "auction_alpha",
    "item_name": "Vintage Rolex Daytona",
    "stock": 1,                     # Exactly ONE item available
    "highest_bid": 100.0,           # Opening price
    "highest_bidder": "reserve_bot",
    "min_increment": 5.0,           # Minimum bump required
    "bids_accepted": 0,
    "bids_rejected": 0,
    "accepted_history": []          # Chronological list of accepted bids
}

class BidRequest(BaseModel):
    bid_id: str
    auction_id: str
    user_id: str
    amount: float

@app.post("/bid")
async def place_bid_naive(bid: BidRequest):
    """
    CRITICAL CONCURRENCY FLAW: Read-Then-Write Anti-Pattern
    1. Reads the highest bid and stock into local stack frame.
    2. Context-switches or yields execution (I/O, DB query, network hop).
    3. During the yield, dozens of other parallel coroutines read the SAME stale values.
    4. Multiple coroutines pass the validation check simultaneously and blindly overwrite.
    """
    # 1. READ: read current state
    current_highest = auction_state["highest_bid"]
    current_stock = auction_state["stock"]
    min_required = current_highest + auction_state["min_increment"]

    # Simulating standard micro-latency: e.g. SQL network roundtrip, JSON parsing, disk seek (1ms)
    # This non-blocking sleep yields control back to the event loop, opening a massive race window!
    await asyncio.sleep(0.001)

    # 2. CHECK: check business rules using stale local variables
    if current_stock <= 0:
        auction_state["bids_rejected"] += 1
        raise HTTPException(status_code=400, detail="Item already sold out")

    if bid.amount < min_required:
        auction_state["bids_rejected"] += 1
        raise HTTPException(
            status_code=400, 
            detail=f"Bid too low. Min required was {min_required}, got {bid.amount}"
        )

    # 3. ACT / WRITE: blind overwrite of shared global state
    # At this point, another coroutine has already updated highest_bid, but this coroutine doesn't know!
    auction_state["highest_bid"] = bid.amount
    auction_state["highest_bidder"] = bid.user_id
    auction_state["bids_accepted"] += 1
    
    # In a naive flash sale, stock decrements per winning bid
    auction_state["stock"] -= 1

    bid_record = {
        "bid_id": bid.bid_id,
        "user_id": bid.user_id,
        "amount": bid.amount,
        "overwritten_from": current_highest,
        "timestamp": time.time()
    }
    auction_state["accepted_history"].append(bid_record)

    return {
        "status": "ACCEPTED",
        "bid_id": bid.bid_id,
        "new_highest_bid": bid.amount,
        "stock_remaining": auction_state["stock"]
    }

@app.get("/stats")
async def get_stats():
    return {
        "auction_id": auction_state["auction_id"],
        "final_highest_bid": auction_state["highest_bid"],
        "final_highest_bidder": auction_state["highest_bidder"],
        "stock": auction_state["stock"],
        "bids_accepted": auction_state["bids_accepted"],
        "bids_rejected": auction_state["bids_rejected"],
        "history_count": len(auction_state["accepted_history"]),
        "accepted_history": auction_state["accepted_history"]
    }

@app.post("/reset")
async def reset_auction():
    auction_state["stock"] = 1
    auction_state["highest_bid"] = 100.0
    auction_state["highest_bidder"] = "reserve_bot"
    auction_state["bids_accepted"] = 0
    auction_state["bids_rejected"] = 0
    auction_state["accepted_history"] = []
    return {"status": "RESET_OK"}
