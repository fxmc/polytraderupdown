"""
Here we provide all the necessary interfaces to resolve markets, traders etc.
These are run at the start-up and don't really need to be asynchronous
"""
import datetime as dt
import requests
import json

import aiohttp

from typing import Any, Protocol, Callable

from analysis.analysis_tools.sessions import fetch_json
from analysis.analysis_tools.consts import GET_MARKET_FROM_SLUG_URL
from analysis.analysis_tools.marketctx import MarketContext

FULL_COIN = {'btc': 'bitcoin', 'eth': 'ethereum', 'xrp': 'xrp', 'sol': 'solana'}


class FetchCallable(Protocol):
    async def __call__(self, url: str, session: Callable[[float], aiohttp.ClientSession], time_out: float = 10.0, trials: int = 1) -> dict[str, Any] | None: ...


def parse_market_api_response(data: dict[str, Any]) -> MarketContext:
    market_details = MarketContext(
        clob_token_ids=json.loads(data['clobTokenIds']),
        end_date=dt.datetime.strptime(data['endDate'], '%Y-%m-%dT%H:%M:%SZ'),
        event_start_time=dt.datetime.strptime(data['eventStartTime'], '%Y-%m-%dT%H:%M:%SZ'),
        market_id=int(data['id']),
        order_min_size=data['orderMinSize'],
        order_price_min_tick_size=data['orderPriceMinTickSize'],
        outcome_prices=list(map(lambda x: float(x), json.loads(data['outcomePrices']))),
        outcomes=json.loads(data['outcomes']),
        question=data['question'],
        condition_id=data['conditionId'],
        slug=data['slug'],
        uma_resolution_status=data.get('umaResolutionStatus'),
        uma_resolution_statuses=json.loads(data['umaResolutionStatuses']) if data.get('umaResolutionStatus') else None,
    )
    return market_details


async def resolve_market_coin_start_time(
        coin: str, start_time: dt.datetime, *,
        fetch: FetchCallable = fetch_json,
        session: Callable[[float], aiohttp.ClientSession],
        market: str = "15m",
        trials: int = 1
) -> MarketContext | None:
    slug = None
    match market:
        case "15m":
            slug = f"{coin.lower()}-updown-15m-{int(start_time.timestamp())}"
        case "1h":
            slug = f"{FULL_COIN[coin.lower()]}-up-or-down-{start_time.strftime('%B').lower()}-{start_time.day}-{int(start_time.strftime('%I'))}{start_time.strftime('%p').lower()}-et"
        case _:
            raise RuntimeError(f"Unknown market {market}")

    url = f"{GET_MARKET_FROM_SLUG_URL}/{slug}"
    data = await fetch(url, session=session, trials=trials)

    if data is None:
        return None

    market_details = parse_market_api_response(data)
    return market_details


def resolve_market_coin_start_time_sync(
        coin: str, start_time: dt.datetime, *,
        market: str = "15m",
) -> MarketContext | None:
    slug = None
    match market:
        case "15m":
            slug = f"{coin.lower()}-updown-15m-{int(start_time.timestamp())}"
        case "1h":
            slug = f"{FULL_COIN[coin.lower()]}-up-or-down-{start_time.strftime('%B').lower()}-{start_time.day}-{int(start_time.strftime('%I'))}{start_time.strftime('%p').lower()}-et"
        case _:
            raise RuntimeError(f"Unknown market {market}")

    url = f"{GET_MARKET_FROM_SLUG_URL}/{slug}"
    resp = requests.get(url)

    if resp is None:
        return None

    market_details = parse_market_api_response(resp.json())
    return market_details
