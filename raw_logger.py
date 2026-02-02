"""
Non-blocking JSONL raw logger.

- log(record) is fire-and-forget: put_nowait into a bounded queue
- writer_task drains queue in batches and appends to a JSONL file
- if queue is full, records are dropped
"""

from __future__ import annotations

import time
import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


class MultiSourceJsonlLogger:
    """
    Routes records to per-source JSONL files.
    Adds run_id + seq to every record.
    """

    def __init__(self, base_dir: str, run_id: str, *, max_queue: int, batch_size: int, flush_every_s: float):
        self.base_dir = base_dir
        self.run_id = run_id
        self.max_queue = max_queue
        self.batch_size = batch_size
        self.flush_every_s = flush_every_s

        self._seq = 0
        self._loggers: dict[str, AsyncJsonlLogger] = {}

    def _get(self, source: str) -> AsyncJsonlLogger:
        if source not in self._loggers:
            path = os.path.join(self.base_dir, self.run_id, f"{source}.jsonl")
            lg = AsyncJsonlLogger.create(
                path=path,
                max_queue=self.max_queue,
                batch_size=self.batch_size,
                flush_every_s=self.flush_every_s,
            )
            lg.start()
            self._loggers[source] = lg
        return self._loggers[source]

    def log(self, record: Dict[str, Any]) -> None:
        source = record.get("source", "unknown")
        self._seq += 1
        record["run_id"] = self.run_id
        record["seq"] = self._seq
        self._get(source).log(record)

    async def stop(self) -> None:
        for lg in self._loggers.values():
            await lg.stop()
        self._loggers.clear()


@dataclass(slots=True)
class AsyncJsonlLogger:
    """Async JSONL logger with bounded queue and background writer task."""
    path: str
    max_queue: int
    batch_size: int
    flush_every_s: float

    _q: asyncio.Queue[str]
    _task: Optional[asyncio.Task[None]]

    @classmethod
    def create(cls, path: str, max_queue: int, batch_size: int, flush_every_s: float) -> "AsyncJsonlLogger":
        """Create a logger and initialize its internal queue."""
        q: asyncio.Queue[str] = asyncio.Queue(maxsize=max_queue)
        return cls(path=path, max_queue=max_queue, batch_size=batch_size, flush_every_s=flush_every_s, _q=q, _task=None)

    def start(self) -> None:
        """Start the background writer task."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._writer_loop())

    def log(self, record: Dict[str, Any]) -> None:
        """Enqueue one JSONL record without blocking; drop if queue is full."""
        try:
            line = json.dumps(record, separators=(",", ":"), ensure_ascii=False, default=str)
        except Exception:
            return

        try:
            self._q.put_nowait(line)
        except asyncio.QueueFull:
            return

    async def stop(self) -> None:
        """Stop the logger and flush remaining queued data."""
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        await self._flush_remaining()

    async def _flush_remaining(self) -> None:
        """Flush remaining queued lines after stop."""
        lines = self._drain_nowait(limit=100_000)
        if not lines:
            return
        await asyncio.to_thread(_append_lines_sync, self.path, lines)

    async def _writer_loop(self) -> None:
        """Drain queue in batches and append to file without blocking the event loop."""
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)

        while True:
            try:
                batch = await self._take_batch()
                if batch:
                    await asyncio.to_thread(_append_lines_sync, self.path, batch)
            except Exception:
                # Do not die; back off briefly and continue.
                await asyncio.sleep(0.25)
                continue

            await asyncio.sleep(self.flush_every_s)

    async def _take_batch(self) -> list[str]:
        """Wait for at least one item, then drain up to batch_size."""
        first = await self._q.get()
        out = [first]
        out.extend(self._drain_nowait(limit=self.batch_size - 1))
        return out

    def _drain_nowait(self, limit: int) -> list[str]:
        """Drain up to limit items from queue without waiting."""
        out: list[str] = []
        for _ in range(limit):
            try:
                out.append(self._q.get_nowait())
            except asyncio.QueueEmpty:
                break
        return out


def _append_lines_sync(path: str, lines: list[str]) -> None:
    """Append a list of JSONL lines to disk (sync)."""
    with open(path, "a", encoding="utf-8", buffering=1) as f:
        for line in lines:
            f.write(line)
            f.write("\n")
