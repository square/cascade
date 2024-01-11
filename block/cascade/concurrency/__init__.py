import asyncio
from typing import Any, Coroutine, TypeVar

T = TypeVar("T")


def run_async(async_fn: Coroutine[Any, Any, T]) -> T:
    """
    Executes a coroutine and blocks until the result is returned.

    This minimal implementation currently supports the case
    where there is a event loop already created in the current thread
    or no event loop exists at all in the current thread.
    """
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    if running_loop:
        task = running_loop.create_task(async_fn)
        return running_loop.run_until_complete(task)
    else:
        return asyncio.run(async_fn)
