import asyncio
import logging
import time

import aiohttp

from typing import Any, Callable, Mapping

from analysis.analysis_tools.consts import GET_MARKET_FROM_SLUG_URL, TRADES_URL, PUBLIC_PROFILE

LOGGER = logging.getLogger(__name__)

GAMMA_API_COUNTER = 0
GAMMA_API_COUNTER_OLD = 0

DATA_API_COUNTER = 0
DATA_API_COUNTER_OLD = 0

START_TIME = time.monotonic()


def make_async_session(timeout: float = 10.0):
    return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout))


async def _read_response_snippet(response: aiohttp.ClientResponse, *, limit: int = 500) -> str:
    """
    Read up to `limit` chars of the response body for debugging.
    Safe for non-JSON bodies.
    """
    try:
        text = await response.text(errors="replace")
        if len(text) > limit:
            return text[:limit] + "…"
        return text
    except Exception as e:
        return f"<failed to read response body: {type(e).__name__}: {e}>"


def _normalize_params(params: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if params is None:
        return None
    # make sure it’s JSON/log friendly (aiohttp accepts many types)
    return dict(params)


async def _fetch_json(url: str, session: Callable[[float], aiohttp.ClientSession], params: dict[str, Any] | None = None, timeout: float = 10.0) -> dict[str, Any] | None:
    global GAMMA_API_COUNTER, GAMMA_API_COUNTER_OLD, DATA_API_COUNTER, DATA_API_COUNTER_OLD, START_TIME

    if url in [GET_MARKET_FROM_SLUG_URL, PUBLIC_PROFILE]:
        GAMMA_API_COUNTER += 1
        LOGGER.info(f"[FETCH-JSON][{asyncio.current_task().get_name()}]GAMMA_API_COUNT = {GAMMA_API_COUNTER}")

    if url in [TRADES_URL]:
        DATA_API_COUNTER += 1
        LOGGER.info(f"DATA_API_COUNTER = {DATA_API_COUNTER}")

    if time.monotonic() - START_TIME > 10:
        START_TIME = time.monotonic()

        if GAMMA_API_COUNTER - GAMMA_API_COUNTER_OLD > 20:  # assuming we run all twelve processes!
            GAMMA_API_COUNTER_OLD = GAMMA_API_COUNTER
            LOGGER.info(f"[FETCH-JSON][{asyncio.current_task().get_name()}] resetting GAMMA API counter and sleeping in this task for 1 second")
            await asyncio.sleep(1)

        if DATA_API_COUNTER - DATA_API_COUNTER_OLD > 12:
            DATA_API_COUNTER_OLD = DATA_API_COUNTER
            LOGGER.info(f"[FETCH-JSON][{asyncio.current_task().get_name()}] resetting DATA API counter and sleeping in this task for 1 second")
            await asyncio.sleep(1)

    norm_params = _normalize_params(params)

    try:
        async with session(timeout) as s:
            async with s.get(url, params=norm_params) as response:
                if response.status < 200 or response.status >= 300:
                    snippet = await _read_response_snippet(response, limit=500)
                    LOGGER.warning("HTTP error response")

                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"Non-2xx response: {snippet}",
                        headers=response.headers,
                    )

                return await response.json()

    except asyncio.CancelledError as e:
        LOGGER.error(f"[{asyncio.current_task().get_name()}]Cancelled Error: {type(e).__name__}: {e}")
        raise

    except aiohttp.ClientResponseError as e:
        # already logged above for non-2xx
        LOGGER.error(f"[{asyncio.current_task().get_name()}]ClientResponseError: {type(e).__name__}: {e}")
        raise

    except aiohttp.ClientError as e:
        LOGGER.error(f"[{asyncio.current_task().get_name()}]HTTP request failed: {type(e).__name__}: {e}")
        raise

    except asyncio.TimeoutError as e:
        LOGGER.error(f"[{asyncio.current_task().get_name()}]TimeOutError: {type(e).__name__}: {e}")
        raise

    except Exception as e:
        LOGGER.error("%sException %s", asyncio.current_task().get_name(), str(e), exc_info=True)
        raise


async def fetch_json(url: str, session: Callable[[float], aiohttp.ClientSession], params: dict[str, Any] | None = None, timeout: float = 10.0, trials: int = 1) -> dict[str, Any] | None:
    if trials < 1:
        return None

    count_down = 0
    back_off = 1
    while True:
        try:
            LOGGER.info(f"[{asyncio.current_task().get_name()}]Attempting to fetch {url} at trial {count_down + 1}")
            result = await _fetch_json(url, session=session, params=params, timeout=timeout)
            LOGGER.info(f"[{asyncio.current_task().get_name()}]!!!!!!!!!!!!!!!!!!!!Successfully fetched {url}! after {count_down + 1} trials!!!!!!!!!!!!!!!!!")
            return result

        except Exception as e:
            LOGGER.error(f"[{asyncio.current_task().get_name()}]Exception: {type(e).__name__}: {e}", exc_info=True)
            count_down += 1
            LOGGER.error(f"[{asyncio.current_task().get_name()}]Tried {count_down} times out of {trials} times")
            LOGGER.error(f"[{asyncio.current_task().get_name()}]Sleeping {back_off} s")
            if count_down == trials:
                LOGGER.exception(f"[{asyncio.current_task().get_name()}]**************************Couldn't get data {url}!  Raising Now!***********************")
                raise
            await asyncio.sleep(back_off)
            back_off = min(back_off * 2, 64)
