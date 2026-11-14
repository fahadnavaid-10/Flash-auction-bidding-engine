"""
FastAPI High-Concurrency Gateway for Flash Auction.
Integrates:
- Redis Lua Atomic Hot Path
- Redis Streams write-ahead logging
- Batched WebSocket broadcast fan-out (50ms coalescing)
- Overload Protection (Rate limiting & Connection ceilings)
- Anti-sniping dynamic soft-close extension
"""

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.app.redis_client import RedisEngine
from backend.app.db import DatabaseManager
from backend.app.websocket_manager import BatchedFanoutManager
from backend.app.overload import OverloadProtector

# Initialize core singletons
redis_engine = RedisEngine()
db_manager = DatabaseManager()
fanout_manager = BatchedFanoutManager(flush_interval_ms=50)
overload = OverloadProtector(max_connections=10000, rate_limit_per_sec=10, rate_limit_burst=15)

pubsub_task: Optional[asyncio.Task] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize connections and background engines
    await redis_engine.connect()
    await db_manager.connect()
    await fanout_manager.start()

    # Seed initial test auction
    await redis_engine.seed_auction(
        auction_id="auction_alpha",
        item_name="Vintage Rolex Daytona 1968",
        initial_price=100.0,
        min_increment=5.0,
        stock=1,
        duration_seconds=300.0
    )

    # Start Redis Pub/Sub subscriber task
    global pubsub_task
    pubsub_task = asyncio.create_task(
        fanout_manager.listen_to_redis_pubsub(redis_engine.client)
    )

    yield

    # Shutdown
    if pubsub_task:
        pubsub_task.cancel()
    await fanout_manager.stop()
    await redis_engine.close()
    await db_manager.close()

app = FastAPI(title="High-Concurrency Flash Auction Engine", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class BidSubmission(BaseModel):
    bid_id: str
    auction_id: str
    user_id: str
    amount: float

@app.post("/bid")
async def place_bid_endpoint(bid: BidSubmission):
    # 1. Overload Protection: Check per-user rate limit
    allowed, reason = overload.check_rate_limit(bid.user_id)
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=reason
        )

    # 2. Atomic Evaluation via Redis Lua Script
    res = await redis_engine.place_bid(
        auction_id=bid.auction_id,
        user_id=bid.user_id,
        bid_id=bid.bid_id,
        amount=bid.amount,
        anti_snipe_window=5.0,
        anti_snipe_extension=10.0
    )

    if res["status"] == "REJECTED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=res["reason"]
        )

    return res

@app.get("/auction/{auction_id}")
async def get_auction_state(auction_id: str):
    if redis_engine.client is None:
        await redis_engine.connect()
    data = await redis_engine.client.hgetall(f"auction:{auction_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Auction not found")

    t = await redis_engine.client.time()
    now = float(t[0]) + (float(t[1]) / 1_000_000.0)
    end_time = float(data.get("end_time", 0.0))
    time_remaining = max(0.0, end_time - now)

    return {
        "auction_id": auction_id,
        "item_name": data.get("item_name"),
        "status": data.get("status"),
        "highest_bid": float(data.get("highest_bid", 0.0)),
        "highest_bidder": data.get("highest_bidder"),
        "min_increment": float(data.get("min_increment", 5.0)),
        "stock": int(data.get("stock", 1)),
        "time_remaining_sec": round(time_remaining, 2),
        "end_time": end_time,
        "bid_count": int(data.get("bid_count", 0))
    }

@app.post("/auction/{auction_id}/reset")
async def reset_auction_endpoint(auction_id: str, duration_sec: float = 300.0):
    res = await redis_engine.seed_auction(
        auction_id=auction_id,
        initial_price=100.0,
        min_increment=5.0,
        stock=1,
        duration_seconds=duration_sec
    )
    return {"status": "RESET_OK", "auction": res}

@app.websocket("/ws/{auction_id}")
async def websocket_auction_endpoint(websocket: WebSocket, auction_id: str):
    # 1. Check Gateway Connection Cap
    if not overload.check_connection_allowed():
        await websocket.close(code=1013, reason="Connection cap reached. Try again later.")
        return

    overload.track_connect()
    await fanout_manager.connect(auction_id, websocket)

    try:
        # Send initial snapshot to newly connected client
        state = await redis_engine.client.hgetall(f"auction:{auction_id}")
        t = await redis_engine.client.time()
        now = float(t[0]) + (float(t[1]) / 1_000_000.0)
        end_time = float(state.get("end_time", 0.0))

        await websocket.send_json({
            "type": "INITIAL_STATE",
            "auction_id": auction_id,
            "highest_bid": float(state.get("highest_bid", 100.0)),
            "highest_bidder": state.get("highest_bidder", "reserve_bot"),
            "min_increment": float(state.get("min_increment", 5.0)),
            "time_remaining": max(0.0, round(end_time - now, 2)),
            "end_time": end_time,
            "status": state.get("status", "ACTIVE")
        })

        # Process incoming bids or pings from client
        while True:
            msg = await websocket.receive_json()
            action = msg.get("action")

            if action == "ping":
                await websocket.send_json({"type": "pong", "time": time.time()})

            elif action == "bid":
                user_id = msg.get("user_id", "anon")
                bid_id = msg.get("bid_id")
                amount = float(msg.get("amount", 0.0))

                allowed, reason = overload.check_rate_limit(user_id)
                if not allowed:
                    await websocket.send_json({"type": "BID_REJECTED", "reason": reason})
                    continue

                res = await redis_engine.place_bid(
                    auction_id=auction_id,
                    user_id=user_id,
                    bid_id=bid_id,
                    amount=amount
                )

                if res["status"] == "ACCEPTED":
                    await websocket.send_json({
                        "type": "BID_CONFIRMED", 
                        "bid_id": bid_id, 
                        "highest_bid": res["highest_bid"]
                    })
                elif res["status"] == "DUPLICATE":
                    await websocket.send_json({
                        "type": "BID_DUPLICATE", 
                        "message": res["message"]
                    })
                else:
                    await websocket.send_json({
                        "type": "BID_REJECTED", 
                        "reason": res["reason"]
                    })

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[!] WebSocket client error: {e}")
    finally:
        overload.track_disconnect()
        await fanout_manager.disconnect(auction_id, websocket)
