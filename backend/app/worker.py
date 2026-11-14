"""
Redis Streams to PostgreSQL Asynchronous Batch Worker.
Consumes confirmed bids from 'auction_stream' using Redis Consumer Groups
and flushes batches to PostgreSQL.
"""

import asyncio
import os
import signal
from typing import List, Dict, Any
from backend.app.redis_client import RedisEngine
from backend.app.db import DatabaseManager

STREAM_KEY = "auction_stream"
GROUP_NAME = "auction_db_consumers"
CONSUMER_NAME = os.getenv("HOSTNAME", "worker-1")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_INTERVAL_MS = int(os.getenv("FLUSH_INTERVAL_MS", "50"))

class StreamConsumerWorker:
    def __init__(self, redis_engine: RedisEngine, db_manager: DatabaseManager):
        self.redis = redis_engine
        self.db = db_manager
        self.running = False

    async def setup_consumer_group(self):
        """Create stream consumer group if it doesn't already exist."""
        try:
            # MKSTREAM creates the stream if it does not exist yet
            await self.redis.client.xgroup_create(
                name=STREAM_KEY, 
                groupname=GROUP_NAME, 
                id="0", 
                mkstream=True
            )
        except Exception as e:
            # BUSYGROUP is returned if group already exists, which is normal
            if "BUSYGROUP" not in str(e):
                pass

    async def process_batch_once(self) -> int:
        """
        Reads up to BATCH_SIZE messages, batch-inserts them into the DB,
        and acknowledges (XACK) them in Redis.
        """
        # Read new messages (id='>') assigned to this consumer
        messages = await self.redis.client.xreadgroup(
            groupname=GROUP_NAME,
            consumername=CONSUMER_NAME,
            streams={STREAM_KEY: ">"},
            count=BATCH_SIZE,
            block=FLUSH_INTERVAL_MS
        )

        if not messages:
            return 0

        # Structure of xreadgroup return: [[stream_name, [(msg_id, {field: val}), ...]]]
        stream_name, stream_entries = messages[0]
        if not stream_entries:
            return 0

        bids_to_insert: List[Dict[str, Any]] = []
        msg_ids_to_ack: List[str] = []

        for msg_id, fields in stream_entries:
            bids_to_insert.append({
                "bid_id": fields["bid_id"],
                "auction_id": fields["auction_id"],
                "user_id": fields["user_id"],
                "amount": float(fields["amount"]),
                "server_time": float(fields["server_time"])
            })
            msg_ids_to_ack.append(msg_id)

        # 1. BATCH PERSIST TO DATABASE (relational ACID write)
        await self.db.insert_bids_batch(bids_to_insert)

        # 2. ACKNOWLEDGE IN REDIS STREAM
        # Only after the database commit succeeds do we mark messages as processed.
        # If the worker crashes before this point, messages remain in the PEL (Pending Entries List)
        # and another consumer will claim and re-process them.
        await self.redis.client.xack(STREAM_KEY, GROUP_NAME, *msg_ids_to_ack)

        return len(bids_to_insert)

    async def run(self):
        self.running = True
        await self.setup_consumer_group()
        print(f"[*] Stream Consumer Worker started [{CONSUMER_NAME}]. Reading from '{STREAM_KEY}'...")

        while self.running:
            try:
                count = await self.process_batch_once()
                if count == 0:
                    await asyncio.sleep(FLUSH_INTERVAL_MS / 1000.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[!] Error in stream consumer worker: {e}")
                await asyncio.sleep(0.5)

    def stop(self):
        self.running = False

async def main():
    redis_engine = RedisEngine()
    db_manager = DatabaseManager()
    await redis_engine.connect()
    await db_manager.connect()

    worker = StreamConsumerWorker(redis_engine, db_manager)
    try:
        await worker.run()
    finally:
        await redis_engine.close()
        await db_manager.close()

if __name__ == "__main__":
    asyncio.run(main())
