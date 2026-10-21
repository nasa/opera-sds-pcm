"""Fan shard work out over processes, each running a thread pool.

Listing and building are bound by Python CPU (botocore request handling, XML parsing), so threads alone top out at
one core. Workers are forked so they inherit the run state (dataset spec, clients factory, burst database) without
pickling it; each worker creates its own S3 clients on first use.
"""
from __future__ import annotations

import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Callable

_STATE: dict = {}


def state() -> dict:
    return _STATE


def split(items: list, parts: int) -> list[list]:
    parts = max(1, min(parts, len(items)))
    return [items[i::parts] for i in range(parts)] if items else []


def thread_map(fn: Callable, items: list, threads: int) -> list:
    if threads <= 1 or len(items) <= 1:
        return [fn(item) for item in items]
    with ThreadPoolExecutor(max_workers=threads) as pool:
        return list(pool.map(fn, items))


def fork_map(task: Callable, groups: list, shared: dict) -> list:
    """`task(group)` in one forked process per group; `shared` is readable in the children through `state()`.
    `task` must be a module-level function."""
    global _STATE
    if not groups:
        return []
    _STATE = shared
    try:
        if len(groups) == 1:
            return [task(groups[0])]
        context = multiprocessing.get_context("fork")
        with ProcessPoolExecutor(max_workers=len(groups), mp_context=context) as pool:
            futures = [pool.submit(task, group) for group in groups]
            return [future.result() for future in futures]
    finally:
        _STATE = {}
