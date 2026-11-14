"""
WebSocket Connection Manager with Batched Fan-Out.
Buffers incoming Redis Pub/Sub broadcast frames and flushes to connected clients
every 50-100ms to eliminate event-loop starvation under high broadcast loads.
"""

import asyncio
import json
import time
from typing import Dict, Set, Optional, Any
from fastapi import WebSocket, WebSocketDisconnect

class BatchedFanoutManager:
    def __init__(self, flush_interval_ms: int = 50):
        # Maps auction_id -> set of active WebSocket instances
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        # Maps auction_id -> latest pending update dict to broadcast
        self.pending_broadcasts: Dict[str, Dict[str, Any]] = {}
        self.flush_interval_sec = flush_interval_ms / 1000.0
        self._flush_task: Optional[asyncio.Task] = None
        self._pubsub_task: Optional[asyncio.Task] = None
        self.lock = asyncio.Lock()
        self.total_broadcasts_sent = 0
        self.total_frames_coalesced = 0

    async def start(self):
        """Start the background 50ms batch flusher."""
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self):
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        if self._pubsub_task:
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass

    async def connect(self, auction_id: str, websocket: WebSocket):
        await websocket.accept()
        async with self.lock:
            if auction_id not in self.active_connections:
                self.active_connections[auction_id] = set()
            self.active_connections[auction_id].add(websocket)

    async def disconnect(self, auction_id: str, websocket: WebSocket):
        async with self.lock:
            if auction_id in self.active_connections:
                self.active_connections[auction_id].discard(websocket)
                if not self.active_connections[auction_id]:
                    del self.active_connections[auction_id]

    def queue_broadcast(self, auction_id: str, update_payload: Dict[str, Any]):
        """
        Coalescing Buffer:
        If multiple price updates arrive within the same 50ms interval,
        we overwrite the pending payload with the latest one.
        This drops redundant intermediate states and saves thousands of socket writes!
        """
        if auction_id in self.pending_broadcasts:
            self.total_frames_coalesced += 1
        self.pending_broadcasts[auction_id] = update_payload

    async def _flush_loop(self):
        """Wakes up every 50ms, snapshots pending updates, and fans out to WebSockets."""
        while True:
            try:
                await asyncio.sleep(self.flush_interval_sec)
                if not self.pending_broadcasts:
                    continue

                # Snapshot and clear pending buffer
                updates = self.pending_broadcasts
                self.pending_broadcasts = {}

                for auction_id, payload in updates.items():
                    clients = list(self.active_connections.get(auction_id, []))
                    if not clients:
                        continue

                    # Pre-serialize to JSON bytes once for all clients (zero re-serialization overhead)
                    raw_message = json.dumps(payload)
                    
                    # Parallel non-blocking fan-out to all sockets in this gateway
                    asyncio.create_task(self._send_to_clients(auction_id, clients, raw_message))
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[!] Error in batched broadcast loop: {e}")

    async def _send_to_clients(self, auction_id: str, clients: list, message: str):
        dead_clients = []
        for ws in clients:
            try:
                await ws.send_text(message)
                self.total_broadcasts_sent += 1
            except Exception:
                dead_clients.append(ws)

        # Cleanup disconnected clients
        if dead_clients:
            async with self.lock:
                for ws in dead_clients:
                    if auction_id in self.active_connections:
                        self.active_connections[auction_id].discard(ws)

    async def listen_to_redis_pubsub(self, redis_client, channel_pattern: str = "auction_channel:*"):
        """Subscribes to Redis Pub/Sub and ingests messages into the coalescing queue."""
        pubsub = redis_client.pubsub()
        await pubsub.psubscribe(channel_pattern)
        print(f"[*] Subscribed to Redis Pub/Sub: {channel_pattern}")

        while True:
            try:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg["type"] == "pmessage":
                    channel = msg["channel"]  # e.g. "auction_channel:auction_alpha"
                    auction_id = channel.split(":")[-1]
                    data = json.loads(msg["data"])
                    self.queue_broadcast(auction_id, data)
            except asyncio.CancelledError:
                break
            except Exception as e:
                await asyncio.sleep(0.5)
