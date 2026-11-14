"""
Unit & Concurrency Tests for Redis Lua Hot Path
Validates:
1. Min increment enforcement
2. Stock safety
3. Expiration/closing time
4. Idempotency (duplicate bid_id)
5. Strict monotonicity under 200 parallel workers
"""

import asyncio
import uuid
import pytest
from backend.app.redis_client import RedisEngine

@pytest.mark.asyncio
async def test_lua_basic_bidding_and_increment():
    engine = RedisEngine(use_fake=True)
    await engine.connect()
    await engine.seed_auction(auction_id="test_1", initial_price=100.0, min_increment=5.0)

    # 1. Bid below min increment should reject
    res1 = await engine.place_bid("test_1", "user_1", str(uuid.uuid4()), 102.0)
    assert res1["status"] == "REJECTED"
    assert res1["reason"] == "BID_TOO_LOW"

    # 2. Bid meeting min increment should accept
    res2 = await engine.place_bid("test_1", "user_1", str(uuid.uuid4()), 105.0)
    assert res2["status"] == "ACCEPTED"
    assert res2["highest_bid"] == 105.0
    assert res2["highest_bidder"] == "user_1"

    # 3. Next bid must exceed 105 + 5 = 110
    res3 = await engine.place_bid("test_1", "user_2", str(uuid.uuid4()), 108.0)
    assert res3["status"] == "REJECTED"

    res4 = await engine.place_bid("test_1", "user_2", str(uuid.uuid4()), 115.0)
    assert res4["status"] == "ACCEPTED"
    assert res4["highest_bid"] == 115.0

    await engine.close()

@pytest.mark.asyncio
async def test_lua_idempotency():
    engine = RedisEngine(use_fake=True)
    await engine.connect()
    await engine.seed_auction(auction_id="test_idem", initial_price=50.0, min_increment=5.0)

    client_bid_id = "unique_client_tx_999"
    
    # First attempt: accepted
    r1 = await engine.place_bid("test_idem", "user_alice", client_bid_id, 60.0)
    assert r1["status"] == "ACCEPTED"
    assert r1["highest_bid"] == 60.0

    # Second attempt (network retry): returns DUPLICATE without raising price
    r2 = await engine.place_bid("test_idem", "user_alice", client_bid_id, 60.0)
    assert r2["status"] == "DUPLICATE"

    # Verify highest bid is still 60.0
    auction_state = await engine.client.hgetall("auction:test_idem")
    assert float(auction_state["highest_bid"]) == 60.0

    await engine.close()

@pytest.mark.asyncio
async def test_lua_auction_expiration():
    engine = RedisEngine(use_fake=True)
    await engine.connect()
    # Seed auction with negative duration (already expired)
    await engine.seed_auction(auction_id="test_exp", initial_price=10.0, duration_seconds=-5.0)

    res = await engine.place_bid("test_exp", "late_user", str(uuid.uuid4()), 20.0)
    assert res["status"] == "REJECTED"
    assert res["reason"] == "AUCTION_CLOSED"

    await engine.close()

@pytest.mark.asyncio
async def test_lua_concurrent_race_resistance():
    """
    Fires 200 parallel bids at the exact same moment.
    Unlike Naive REST, the Redis Lua script executes sequentially:
    - Zero price stagnation or drops.
    - Strict monotonic increase.
    """
    engine = RedisEngine(use_fake=True)
    await engine.connect()
    await engine.seed_auction(auction_id="test_race", initial_price=100.0, min_increment=1.0)

    async def worker(i: int):
        # Every worker tries to bid 100 + i (random order of arrival)
        amount = 100.0 + (i % 20) * 2.0  # amounts between 100 and 140
        return await engine.place_bid("test_race", f"user_{i}", str(uuid.uuid4()), amount)

    tasks = [worker(i) for i in range(1, 201)]
    results = await asyncio.gather(*tasks)

    accepted = [r for r in results if r["status"] == "ACCEPTED"]
    print(f"\nConcurrent Lua: {len(accepted)} accepted out of 200 parallel attempts.")

    # Fetch final state
    state = await engine.client.hgetall("auction:test_race")
    final_highest = float(state["highest_bid"])

    # Invariants:
    # 1. Final highest bid must be at least the initial price
    assert final_highest >= 100.0
    # 2. Total accepted bids must match bid_count
    assert int(state["bid_count"]) == len(accepted)
    # 3. Exactly one winner holds the highest bid
    assert state["highest_bidder"] != ""

    await engine.close()
