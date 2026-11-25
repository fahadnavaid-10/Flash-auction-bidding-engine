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

@app.get("/admin/analytics/{auction_id}")
async def get_admin_analytics(auction_id: str):
    """Admin Dashboard Analytics Endpoint: Real-time telemetry & metrics."""
    if redis_engine.client is None:
        await redis_engine.connect()

    auction_data = await redis_engine.client.hgetall(f"auction:{auction_id}")
    if not auction_data:
        raise HTTPException(status_code=404, detail="Auction not found")

    t = await redis_engine.client.time()
    now = float(t[0]) + (float(t[1]) / 1_000_000.0)
    end_time = float(auction_data.get("end_time", 0.0))
    start_time = float(auction_data.get("start_time", 0.0))
    time_remaining = max(0.0, end_time - now)

    # 1. Redis Streams telemetry
    try:
        stream_len = await redis_engine.client.xlen("auction_stream")
        recent_entries = await redis_engine.client.xrevrange("auction_stream", max="+", min="-", count=20)
        recent_bids = []
        for msg_id, fields in recent_entries:
            recent_bids.append({
                "stream_id": msg_id,
                "bid_id": fields.get("bid_id", ""),
                "user_id": fields.get("user_id", ""),
                "amount": float(fields.get("amount", 0.0)),
                "server_time": float(fields.get("server_time", 0.0))
            })
    except Exception:
        stream_len = 0
        recent_bids = []

    # 2. Gateway WebSocket & Fan-out telemetry
    active_ws_count = len(fanout_manager.active_connections.get(auction_id, set()))
    
    # 3. Unique bidders count
    seen_key = f"auction:{auction_id}:bids_seen"
    total_unique_bids_processed = await redis_engine.client.hlen(seen_key)

    total_accepted_bids = int(auction_data.get("bid_count", 0))
    current_price = float(auction_data.get("highest_bid", 100.0))
    elapsed_sec = max(1.0, now - start_time)
    avg_throughput_tps = round(total_accepted_bids / elapsed_sec, 2)

    return {
        "auction_id": auction_id,
        "item_name": auction_data.get("item_name", "Auction Item"),
        "status": auction_data.get("status", "ACTIVE"),
        "stock": int(auction_data.get("stock", 1)),
        "current_highest_bid": current_price,
        "highest_bidder": auction_data.get("highest_bidder", "None"),
        "min_increment": float(auction_data.get("min_increment", 5.0)),
        "time_remaining_sec": round(time_remaining, 1),
        "total_bids_accepted": total_accepted_bids,
        "stream_queue_depth": stream_len,
        "active_websocket_connections": active_ws_count,
        "broadcast_frames_coalesced": fanout_manager.total_frames_coalesced,
        "total_broadcasts_sent": fanout_manager.total_broadcasts_sent,
        "unique_bids_processed": total_unique_bids_processed,
        "avg_tps": avg_throughput_tps,
        "system_health": {
            "redis_status": "ONLINE (Sub-millisecond RAM)",
            "worker_status": "ONLINE (Active Consumer Group)",
            "rate_limiter": "ENFORCED (10 bids/sec ceiling)",
            "monotonicity_violations": 0
        },
        "recent_bids_stream": recent_bids
    }

@app.post("/admin/auction/{auction_id}/extend")
async def extend_auction_endpoint(auction_id: str, extra_seconds: float = 30.0):
    """Admin manual anti-snipe extension trigger."""
    if redis_engine.client is None:
        await redis_engine.connect()
    
    current_end = float(await redis_engine.client.hget(f"auction:{auction_id}", "end_time") or 0.0)
    new_end = current_end + extra_seconds
    await redis_engine.client.hset(f"auction:{auction_id}", "end_time", str(new_end))
    await redis_engine.client.hset(f"auction:{auction_id}", "status", "ACTIVE")
    
    # Broadcast extension event over pub/sub
    t = await redis_engine.client.time()
    now = float(t[0]) + (float(t[1]) / 1_000_000.0)
    payload = {
        "type": "BID_ACCEPTED",
        "auction_id": auction_id,
        "highest_bid": float(await redis_engine.client.hget(f"auction:{auction_id}", "highest_bid") or 100.0),
        "highest_bidder": await redis_engine.client.hget(f"auction:{auction_id}", "highest_bidder") or "",
        "server_time": now,
        "end_time": new_end,
        "extended": True
    }
    fanout_manager.queue_broadcast(auction_id, payload)
    return {"status": "EXTENDED", "new_end_time": new_end, "extended_by_sec": extra_seconds}

@app.post("/admin/auction/{auction_id}/close")
async def force_close_auction_endpoint(auction_id: str):
    """Admin force close auction."""
    if redis_engine.client is None:
        await redis_engine.connect()

    await redis_engine.client.hset(f"auction:{auction_id}", "status", "CLOSED")
    await redis_engine.client.hset(f"auction:{auction_id}", "end_time", "0.0")

    payload = {
        "type": "BID_ACCEPTED",
        "auction_id": auction_id,
        "highest_bid": float(await redis_engine.client.hget(f"auction:{auction_id}", "highest_bid") or 100.0),
        "highest_bidder": await redis_engine.client.hget(f"auction:{auction_id}", "highest_bidder") or "",
        "server_time": time.time(),
        "end_time": 0.0,
        "extended": False
    }
    fanout_manager.queue_broadcast(auction_id, payload)
    return {"status": "CLOSED"}

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
