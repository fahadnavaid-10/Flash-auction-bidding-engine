"""
Test Redis Streams to Database Durability & Batch Worker.
Validates:
1. Bids appended to stream upon acceptance
2. Consumer group reads and flushes in batches
3. Exactly-once persistence without bid loss
4. XACK acknowledgement removes messages from Pending Entries List (PEL)
"""

import asyncio
import uuid
import pytest
from backend.app.redis_client import RedisEngine
from backend.app.db import DatabaseManager
from backend.app.worker import StreamConsumerWorker, STREAM_KEY, GROUP_NAME

@pytest.mark.asyncio
async def test_stream_batch_persistence():
    redis_engine = RedisEngine(use_fake=True)
    db_manager = DatabaseManager(dsn="sqlite:///:memory:")
    await redis_engine.connect()
    await db_manager.connect()

    auction_id = "test_stream_auction"
    await redis_engine.seed_auction(auction_id=auction_id, initial_price=100.0, min_increment=5.0)
    await db_manager.seed_auction(auction_id=auction_id, item_name="Test Item", initial_price=100.0, min_increment=5.0)

    # 1. Place 10 sequential valid bids
    placed_bids = []
    current_amount = 100.0
    for i in range(1, 11):
        current_amount += 5.0
        bid_id = str(uuid.uuid4())
        user_id = f"user_{i}"
        res = await redis_engine.place_bid(auction_id, user_id, bid_id, current_amount)
        assert res["status"] == "ACCEPTED"
        placed_bids.append({"bid_id": bid_id, "amount": current_amount, "user_id": user_id})

    # Verify stream has 10 entries
    stream_len = await redis_engine.client.xlen(STREAM_KEY)
    assert stream_len == 10

    # 2. Run the Worker Batch Processor
    worker = StreamConsumerWorker(redis_engine, db_manager)
    await worker.setup_consumer_group()

    processed_count = await worker.process_batch_once()
    assert processed_count == 10

    # 3. Verify Database State
    db_bids = await db_manager.get_bids(auction_id)
    assert len(db_bids) == 10

    for i, expected in enumerate(placed_bids):
        actual = db_bids[i]
        assert actual["bid_id"] == expected["bid_id"]
        assert actual["amount"] == expected["amount"]
        assert actual["user_id"] == expected["user_id"]

    # 4. Verify PEL (Pending Entries List) is empty due to XACK
    pel_info = await redis_engine.client.xpending(STREAM_KEY, GROUP_NAME)
    # pel_info['pending'] should be 0
    pending_count = pel_info["pending"] if isinstance(pel_info, dict) else pel_info[0]
    assert pending_count == 0

    await redis_engine.close()
    await db_manager.close()
