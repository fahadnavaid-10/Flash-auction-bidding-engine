"""
Invariant Verification Test Suite.
Tests the core non-negotiable invariants of the Flash Auction Engine:
1. Stock safety: Stock never oversold (stock >= 0 always).
2. Monotonicity: Highest bid never decreases (B_{n+1} >= B_n + min_increment).
3. Deterministic Winner: Exactly one unambiguous winner under concurrent ties.
4. Idempotency: Duplicate bid_id retries produce no duplicate price increments.
"""

import asyncio
import uuid
import pytest
from backend.app.redis_client import RedisEngine
from backend.app.worker import STREAM_KEY

@pytest.mark.asyncio
async def test_invariant_stock_never_oversold():
    """
    INVARIANT 1: STOCK NEVER OVERSOLD
    Even when 300 parallel bidders compete for 1 item, stock is never oversold.
    """
    engine = RedisEngine(use_fake=True)
    await engine.connect()
    auction_id = "test_stock_safety"
    await engine.seed_auction(auction_id=auction_id, initial_price=100.0, min_increment=1.0, stock=1)

    # 300 coroutines firing bids simultaneously
    async def place_worker(idx: int):
        # All attempt to place a bid
        amt = 100.0 + idx * 2.0
        return await engine.place_bid(auction_id, f"bidder_{idx}", str(uuid.uuid4()), amt)

    tasks = [place_worker(i) for i in range(1, 301)]
    results = await asyncio.gather(*tasks)

    state = await engine.client.hgetall(f"auction:{auction_id}")
    final_stock = int(state.get("stock", 0))

    # Assert stock is never negative
    assert final_stock >= 0, f"Violation: Stock went negative ({final_stock})!"
    assert final_stock == 1  # For an auction, stock is 1 unit awarded to the highest bidder at the end
    await engine.close()

@pytest.mark.asyncio
async def test_invariant_highest_bid_never_decreases():
    """
    INVARIANT 2: STRICT PRICE MONOTONICITY
    Every accepted bid must strictly exceed the previous bid by at least min_increment.
    Monotonicity violations must be exactly ZERO.
    """
    engine = RedisEngine(use_fake=True)
    await engine.connect()
    auction_id = "test_monotonicity"
    min_increment = 5.0
    await engine.seed_auction(auction_id=auction_id, initial_price=100.0, min_increment=min_increment)

    # Clear stream to inspect this auction's bids
    await engine.client.delete(STREAM_KEY)

    # 250 parallel workers offering disordered bid amounts
    import random
    amounts = [105.0 + i * random.choice([1.0, 5.0, 10.0]) for i in range(250)]
    random.shuffle(amounts)

    async def place_worker(amt: float, idx: int):
        return await engine.place_bid(auction_id, f"user_{idx}", str(uuid.uuid4()), amt)

    tasks = [place_worker(amt, i) for i, amt in enumerate(amounts)]
    results = await asyncio.gather(*tasks)

    # Inspect the accepted stream order
    entries = await engine.client.xrange(STREAM_KEY)
    accepted_bids = [float(fields["amount"]) for msg_id, fields in entries if fields["auction_id"] == auction_id]

    print(f"\nAccepted {len(accepted_bids)} bids monotonically out of 250 attempts.")
    assert len(accepted_bids) > 0

    # Invariant check: B_{i} >= B_{i-1} + min_increment
    monotonicity_violations = 0
    for i in range(1, len(accepted_bids)):
        prev_amt = accepted_bids[i - 1]
        curr_amt = accepted_bids[i]
        if curr_amt < prev_amt + min_increment:
            monotonicity_violations += 1

    assert monotonicity_violations == 0, f"Violation: {monotonicity_violations} bids violated monotonicity!"

    # Verify final auction state matches the last accepted stream entry
    state = await engine.client.hgetall(f"auction:{auction_id}")
    assert float(state["highest_bid"]) == accepted_bids[-1]

    await engine.close()

@pytest.mark.asyncio
async def test_invariant_deterministic_winner():
    """
    INVARIANT 3: DETERMINISTIC WINNER
    When 100 bidders submit the identical high amount at the exact same moment,
    exactly ONE bidder is declared the winner (first arrival in Lua).
    The remaining 99 are rejected without ambiguous or split states.
    """
    engine = RedisEngine(use_fake=True)
    await engine.connect()
    auction_id = "test_deterministic_winner"
    await engine.seed_auction(auction_id=auction_id, initial_price=100.0, min_increment=5.0)

    # 100 bidders all submitting identical bid of 110.0 simultaneously
    identical_amount = 110.0

    async def tie_worker(idx: int):
        return await engine.place_bid(auction_id, f"tied_user_{idx}", str(uuid.uuid4()), identical_amount)

    tasks = [tie_worker(i) for i in range(100)]
    results = await asyncio.gather(*tasks)

    accepted = [r for r in results if r["status"] == "ACCEPTED"]
    rejected = [r for r in results if r["status"] == "REJECTED"]

    # Exactly ONE bid can be accepted at this amount!
    assert len(accepted) == 1, f"Expected exactly 1 winner for amount 110.0, got {len(accepted)}!"
    assert len(rejected) == 99

    # The winner in state must match the accepted result
    state = await engine.client.hgetall(f"auction:{auction_id}")
    assert state["highest_bidder"] == accepted[0]["highest_bidder"]
    assert float(state["highest_bid"]) == identical_amount

    await engine.close()

@pytest.mark.asyncio
async def test_invariant_idempotency_on_retry():
    """
    INVARIANT 4: IDEMPOTENCY ON CLIENT RETRY
    Rapid double-clicks or network timeout retries with the same bid_id
    MUST NOT increment the price or create duplicate entries.
    """
    engine = RedisEngine(use_fake=True)
    await engine.connect()
    auction_id = "test_idempotency_retry"
    await engine.seed_auction(auction_id=auction_id, initial_price=100.0, min_increment=5.0)

    fixed_bid_id = f"client_tx_{uuid.uuid4()}"
    bid_amount = 150.0

    # Simulate 20 concurrent retries of the SAME request
    tasks = [
        engine.place_bid(auction_id, "alice", fixed_bid_id, bid_amount)
        for _ in range(20)
    ]
    results = await asyncio.gather(*tasks)

    accepted = [r for r in results if r["status"] == "ACCEPTED"]
    duplicates = [r for r in results if r["status"] == "DUPLICATE"]

    assert len(accepted) == 1, f"Expected 1 ACCEPTED, got {len(accepted)}"
    assert len(duplicates) == 19, f"Expected 19 DUPLICATE, got {len(duplicates)}"

    # Highest bid must be 150.0, bid_count must be exactly 1
    state = await engine.client.hgetall(f"auction:{auction_id}")
    assert float(state["highest_bid"]) == 150.0
    assert int(state["bid_count"]) == 1

    await engine.close()
