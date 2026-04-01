from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

_LIVE_TASKS: set[asyncio.Task] = set()


def fire_and_forget(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _LIVE_TASKS.add(task)
    task.add_done_callback(_LIVE_TASKS.discard)
    return task


def fire_and_forget_sync(func: Callable[..., Any], *args, **kwargs) -> asyncio.Task:
    return fire_and_forget(asyncio.to_thread(func, *args, **kwargs))


async def run_sync(func: Callable[..., Any], *args, **kwargs) -> Any:
    return await asyncio.to_thread(func, *args, **kwargs)
