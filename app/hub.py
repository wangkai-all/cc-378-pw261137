"""In-process pub/sub hub bridging durable outbox and live SSE clients.

Delivery contract: *at-least-once*.

* Every main-line event is written to main_events and to each subscription's
  outbox in the same DB transaction (durable before any client is told).
* Live clients are pushed through an asyncio queue; the outbox row is only
  marked delivered after a frame is produced.
* With the `drop_notification_after_commit` fault enabled, the live push is
  swallowed exactly as if the network/notification died. The event stays
  undelivered and resurfaces on reconnect or via the replay endpoint —
  the simulated "commit succeeded but notification was lost" scenario.
* Clients reconnect with ?last_seq=N and receive the full backlog first, so a
  merge_switch marker is never skipped even if the connection dropped exactly
  at the cutover point.
"""
from __future__ import annotations

import asyncio
import json
import threading

from .storage import Store


class SubscriptionHub:
    def __init__(self, store: Store):
        self.store = store
        # subscription_id -> set of asyncio queues held by live SSE streams
        self._clients: dict[int, set[asyncio.Queue]] = {}
        self._lock = threading.Lock()
        # Bridge for request-handler threads (sync) -> asyncio loop.
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready = threading.Event()

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._ready.set()

    def register(self, sub_id: int) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        with self._lock:
            self._clients.setdefault(sub_id, set()).add(q)
        return q

    def unregister(self, sub_id: int, q: asyncio.Queue) -> None:
        with self._lock:
            self._clients.get(sub_id, set()).discard(q)

    def publish(self, sub_id: int, event: dict) -> None:
        """Called from sync request threads after a main-line commit."""
        if self.store.get_fault("drop_notification_after_commit", False):
            return  # swallow: outbox stays undelivered, backlog path resends
        self._ready.wait(timeout=5)
        with self._lock:
            clients = list(self._clients.get(sub_id, set()))
        if not clients:
            return

        def _enqueue() -> None:
            for q in clients:
                try:
                    q.put_nowait(event)
                except asyncio.QueueFull:
                    pass  # slow consumer: durable backlog covers it on reconnect

        self._loop.call_soon_threadsafe(_enqueue)

    def publish_all(self, event: dict) -> None:
        for sub in self.store.list_subscriptions():
            self.publish(sub["id"], event)

    async def stream(self, sub_id: int, last_seq: int):
        """Async generator yielding SSE frames for one client."""
        q = self.register(sub_id)
        try:
            # 1) Durable backlog first (covers reconnects and missed pushes).
            backlog = await asyncio.to_thread(self.store.outbox_pending, sub_id)
            for event in backlog:
                if event["seq"] <= last_seq:
                    await asyncio.to_thread(
                        self.store.mark_delivered, sub_id, event["seq"])
                    continue
                yield self._sse(event)
                await asyncio.to_thread(
                    self.store.mark_delivered, sub_id, event["seq"])

            # 2) Live tail, fully async so frames flush immediately.
            while True:
                try:
                    event = await asyncio.wait_for(q.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                delivered = await asyncio.to_thread(
                    self.store.last_delivered_seq, sub_id)
                if event["seq"] <= delivered:
                    continue  # duplicate guard across reconnect races
                yield self._sse(event)
                await asyncio.to_thread(
                    self.store.mark_delivered, sub_id, event["seq"])
        finally:
            self.unregister(sub_id, q)

    @staticmethod
    def _sse(event: dict) -> str:
        return (f"id: {event['seq']}\n"
                f"data: {json.dumps(event, ensure_ascii=False)}\n\n")
