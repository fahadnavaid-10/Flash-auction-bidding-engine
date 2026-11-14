"""
Database Engine & Persistence Layer.
Supports PostgreSQL (asyncpg) in Docker/Production and SQLite fallback for local test execution.
"""

import os
import sqlite3
from typing import List, Dict, Any, Optional

class DatabaseManager:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.getenv(
            "DATABASE_URL", 
            "postgresql://auction_user:auction_pass@localhost:5432/auction_db"
        )
        self.is_sqlite = False
        self._pg_pool = None
        self._sqlite_conn = None

    async def connect(self):
        """Attempt connection to PostgreSQL, with transparent SQLite fallback for tests."""
        if "sqlite" in self.dsn:
            self.is_sqlite = True
            self._init_sqlite()
            return

        try:
            import asyncpg
            self._pg_pool = await asyncpg.create_pool(self.dsn, min_size=5, max_size=20, timeout=3.0)
            self.is_sqlite = False
        except Exception:
            # Fallback to local SQLite for isolated non-Docker testing
            self.is_sqlite = True
            self._init_sqlite()

    def _init_sqlite(self):
        # In-memory SQLite with full relational schema
        self._sqlite_conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._sqlite_conn.row_factory = sqlite3.Row
        cur = self._sqlite_conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS auctions (
                id TEXT PRIMARY KEY,
                item_name TEXT NOT NULL,
                start_time REAL NOT NULL,
                end_time REAL NOT NULL,
                initial_price REAL NOT NULL,
                min_increment REAL NOT NULL,
                current_highest_bid REAL NOT NULL,
                highest_bidder TEXT NOT NULL,
                stock INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'ACTIVE'
            );

            CREATE TABLE IF NOT EXISTS bids (
                bid_id TEXT PRIMARY KEY,
                auction_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                amount REAL NOT NULL,
                server_timestamp REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self._sqlite_conn.commit()

    async def close(self):
        if self._pg_pool:
            await self._pg_pool.close()
        if self._sqlite_conn:
            self._sqlite_conn.close()

    async def seed_auction(self, auction_id: str, item_name: str, initial_price: float, min_increment: float, stock: int = 1):
        if self.is_sqlite:
            cur = self._sqlite_conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO auctions (id, item_name, start_time, end_time, initial_price, min_increment, current_highest_bid, highest_bidder, stock, status)
                VALUES (?, ?, 0.0, 9999999999.0, ?, ?, ?, 'reserve_bot', ?, 'ACTIVE')
            """, (auction_id, item_name, initial_price, min_increment, initial_price, stock))
            self._sqlite_conn.commit()
        else:
            async with self._pg_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO auctions (id, item_name, start_time, end_time, initial_price, min_increment, current_highest_bid, highest_bidder, stock, status)
                    VALUES ($1, $2, 0.0, 9999999999.0, $3, $4, $5, 'reserve_bot', $6, 'ACTIVE')
                    ON CONFLICT (id) DO UPDATE SET current_highest_bid = EXCLUDED.current_highest_bid;
                """, auction_id, item_name, initial_price, min_increment, initial_price, stock)

    async def insert_bids_batch(self, bids: List[Dict[str, Any]]):
        """
        Batch-inserts accepted bids from Redis Stream into the relational database.
        Runs inside a single atomic transaction.
        """
        if not bids:
            return 0

        if self.is_sqlite:
            cur = self._sqlite_conn.cursor()
            cur.executemany("""
                INSERT OR IGNORE INTO bids (bid_id, auction_id, user_id, amount, server_timestamp)
                VALUES (:bid_id, :auction_id, :user_id, :amount, :server_time)
            """, bids)
            
            # Update auction latest highest bid
            latest = bids[-1]
            cur.execute("""
                UPDATE auctions 
                SET current_highest_bid = ?, highest_bidder = ?
                WHERE id = ? AND ? > current_highest_bid
            """, (latest["amount"], latest["user_id"], latest["auction_id"], latest["amount"]))
            self._sqlite_conn.commit()
            return len(bids)
        else:
            async with self._pg_pool.acquire() as conn:
                async with conn.transaction():
                    records = [
                        (b["bid_id"], b["auction_id"], b["user_id"], b["amount"], b["server_time"])
                        for b in bids
                    ]
                    await conn.executemany("""
                        INSERT INTO bids (bid_id, auction_id, user_id, amount, server_timestamp)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (bid_id) DO NOTHING;
                    """, records)

                    latest = bids[-1]
                    await conn.execute("""
                        UPDATE auctions 
                        SET current_highest_bid = $1, highest_bidder = $2
                        WHERE id = $3 AND $1 > current_highest_bid;
                    """, latest["amount"], latest["user_id"], latest["auction_id"])
                    return len(bids)

    async def get_bids(self, auction_id: str) -> List[Dict[str, Any]]:
        if self.is_sqlite:
            cur = self._sqlite_conn.cursor()
            cur.execute("SELECT * FROM bids WHERE auction_id = ? ORDER BY server_timestamp ASC", (auction_id,))
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        else:
            async with self._pg_pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM bids WHERE auction_id = $1 ORDER BY server_timestamp ASC", auction_id)
                return [dict(r) for r in rows]
