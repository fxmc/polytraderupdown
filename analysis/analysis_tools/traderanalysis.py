import asyncio
import datetime as dt
import aiohttp
import pandas as pd
import polars as pl
import numpy as np
import logging

from typing import Sequence, Callable, Any

from dataclasses import dataclass

import requests

from analysis.analysis_tools.sessions import fetch_json, make_async_session
from analysis.analysis_tools.consts import TRADES_URL, NYC, PUBLIC_PROFILE
from analysis.analysis_tools.marketctx import MarketContext
from analysis.analysis_tools.marketresolution import resolve_market_coin_start_time, resolve_market_coin_start_time_sync

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(level=logging.WARNING)


@dataclass
class MarketOutcome:
    date: dt.datetime
    proxy_wallet: str
    name: str
    market_id: str
    slug: str
    question: str
    pnl: float
    taker_maker_ratio: float | None
    num_trades_api: int
    up_price: float


def _create_params(market: str, limit: int, offset: int, taker_only: bool, wallet: str | None) -> dict:
    params = {
            "market": market,
            "limit": limit,
            "offset": offset,
            "takerOnly": str(taker_only).lower(),
        }

    if wallet is not None:
        params['user'] = wallet

    return params


def label_taker_trades(df_all: pd.DataFrame, df_taker: pd.DataFrame) -> tuple[pd.DataFrame | None, float | None]:
    """Label trades in ``df_all`` as "taker" when they appear in ``df_taker``."""
    if df_all is None or df_all.empty:
        return pd.DataFrame(), np.nan   # <- wow, can happen that df_taker is non-empty, but df_all is empty, WTF?

    # Merge taker trades to label them
    if df_taker is None or df_taker.empty:
        df_all_with_label = df_all.assign(taker_flag="maker")
        df_taker = pd.DataFrame()
    else:
        df_all_with_label = df_all.merge(
            df_taker.assign(taker_flag="taker")[["date", "size", "outcome", "price", "transactionHash", "proxyWallet", "taker_flag"]],
            how="left",
            on=["date", "size", "outcome", "price", "transactionHash", "proxyWallet"],
        )

    final_count = len(df_all_with_label.query('taker_flag == "taker"'))
    df_all_with_label["taker_flag"] = df_all_with_label["taker_flag"].fillna("maker")

    # assert final_count == len(df_all_with_label)
    taker_maker_ratio = len(df_taker) / len(df_all)
    print(f"taker length = {len(df_taker)}, final result taker count = {final_count}, taker_maker_ratio = {taker_maker_ratio}")

    return df_all_with_label, taker_maker_ratio


def determine_resolution(market_dict: MarketContext) -> float:
    up_price = float(market_dict.outcome_prices[0])
    return up_price


def calculate_pl(user_full_trades: pl.DataFrame, up_price: float) -> float:
    market_acc = (
        user_full_trades
        .with_columns(
            pl.when(pl.col("side") == "BUY").then(1.0).otherwise(-1.0).alias('sign')
        )
        .with_columns(
            (pl.col('sign') * pl.when(pl.col('outcome') == "Up").then(pl.col('size')).otherwise(0.0)).alias('UpShares'),
            (pl.col('sign') * pl.when(pl.col('outcome') == "Down").then(pl.col('size')).otherwise(0.0)).alias('DownShares'),
        )
        .with_columns(
            (pl.col('UpShares') * pl.col('price')).alias('UpPaid'),
            (pl.col('DownShares') * pl.col('price')).alias('DownPaid')

        )
        .select('UpShares', 'UpPaid', 'DownShares', 'DownPaid')
        .sum()
    )

    total_paid = (market_acc['UpPaid'] + market_acc['DownPaid'])[0]

    if up_price == 1:
        pnl = market_acc['UpShares'][0] - total_paid
    else:
        pnl = market_acc['DownShares'][0] - total_paid

    return pnl


# ==== sync versions
def get_trades_sync(market: str, *, wallet: str | None, limit: int = 500, taker_only: bool = False) -> pd.DataFrame | None:
    res: list[pd.DataFrame] = []
    offset = 0
    start_time = np.nan

    LOGGER.info(f">>>> Starting Grabbing trades for {market} at offset={offset} with taker_only={taker_only}")
    while True:
        params = _create_params(market, limit, offset, taker_only, wallet)
        LOGGER.info(f"{params!r}")
        data = requests.get(TRADES_URL, params=params).json()

        if not data:
            LOGGER.info(f"No more trades returned for market {market} (taker_only={taker_only})")
            break

        if len(data) == 0:
            LOGGER.info(f"Response empty, finished retrieving all for taker_only = {taker_only}")
            break

        if data[0]["timestamp"] >= start_time:
            LOGGER.info(f'Data repeats, finished retrieving all for taker_only = {taker_only}')
            break

        start_time = data[0]['timestamp']

        df = pd.DataFrame(data)
        res.append(df)
        offset += limit

    LOGGER.info(f">>>> Finished Grabbing trades for {market} at offset={offset} with taker_only={taker_only}")

    if len(res) == 0:
        LOGGER.info(f"number of data frames captured = {len(res)}")
        return None

    return (
        pd.concat(res, ignore_index=True)
        .assign(date=lambda x: pd.to_datetime(x["timestamp"], unit="s", utc=True).dt.tz_convert(NYC))
        .assign(
            size=lambda x: x['size'].astype(float),
            price=lambda x: x['price'].astype(float)
        )
        .sort_values(by="date")
    )


def grab_trades_for_trader_market_sync(proxy_wallet: str, condition_id: str, trials: int = 1) -> tuple[pd.DataFrame | None, float | None]:
    df_maker = get_trades_sync(condition_id, wallet=proxy_wallet, taker_only=False)

    if df_maker is None:    # since df_maker returns all trades, difficult to have taker trades if this is None!
        LOGGER.info(f"No trades for condition id: {condition_id} and proxy_wallet: {proxy_wallet}")
        return None, None

    df_taker = get_trades_sync(condition_id, wallet=proxy_wallet, taker_only=True)
    return label_taker_trades(df_maker, df_taker)


def get_trader_information_sync(proxy_wallet: str) -> dict[str, Any] | None:
    params = {
        "address": proxy_wallet,
    }
    data: dict[str, Any] = requests.get(PUBLIC_PROFILE, params=params).json()
    return data


def get_market_outcome_sync(proxy_wallet: str, coin: str, start_date: dt.datetime) -> tuple[MarketOutcome, pd.DataFrame] | tuple[None, None]:
    market_dict = resolve_market_coin_start_time_sync(coin, start_date)
    trader_dict = get_trader_information_sync(proxy_wallet)
    market_id: str = market_dict.condition_id
    df_trades, taker_maker_ratio = grab_trades_for_trader_market_sync(proxy_wallet, market_id)

    if df_trades is None or len(df_trades) == 0:
        return None, None

    user_full_trades = pl.from_pandas(
        df_trades
        [['date', 'side', 'size', 'price', 'outcome', 'name', 'taker_flag', 'transactionHash', 'proxyWallet', 'conditionId', 'asset', 'title', 'slug']]
    )
    up_price = determine_resolution(market_dict)
    pnl: float = calculate_pl(user_full_trades, up_price)
    name: str = trader_dict['name']
    question: str = market_dict.question
    return MarketOutcome(
        start_date,
        proxy_wallet,
        name,
        market_id,
        market_dict.slug,
        question,
        pnl,
        taker_maker_ratio,
        len(df_trades),
        market_dict.outcome_prices[0]
    ), user_full_trades


def grab_trades_for_trader_sync(proxy_wallet: str, coin: str, start_date: dt.datetime, end_date: dt.datetime) -> pd.DataFrame:
    date = start_date
    res = []
    while True:
        print(f"Fetching data for {date}")
        trade_results = get_market_outcome_sync(proxy_wallet, coin, start_date)
        if trade_results:
            res.append(trade_results)
        date = date + dt.timedelta(minutes=15)
        if date >= end_date:
            break

    pnl = pd.DataFrame(res)
    return pnl


# ==== async versions
async def get_trades(session: Callable[[float], aiohttp.ClientSession], market: str, *,
                     wallet: str | None = None, limit: int = 500, taker_only: bool = False, timeout: float = 10.0, trials: int = 1) -> pd.DataFrame | None:

    res: list[pd.DataFrame] = []
    offset = 0
    start_time = np.nan

    LOGGER.info(f">>>> Starting Grabbing trades for {market} at offset={offset} with taker_only={taker_only}")
    while True:
        params = _create_params(market, limit, offset, taker_only, wallet)
        LOGGER.info(f"{params!r}")
        try:
            data: Sequence[dict] = await fetch_json(TRADES_URL, session, params=params, timeout=timeout, trials=trials)
        except Exception as exc:
            LOGGER.warning(f"[{asyncio.current_task().get_name()}]Failed to fetch trades for market {market} offset={offset}: {exc}")
            break

        if not data:
            LOGGER.info(f"No more trades returned for market {market} (taker_only={taker_only})")
            break

        if len(data) == 0:
            LOGGER.info(f"Response empty, finished retrieving all for taker_only = {taker_only}")
            break

        if data[0]["timestamp"] >= start_time:
            LOGGER.info(f'Data repeats, finished retrieving all for taker_only = {taker_only}')
            break

        start_time = data[0]['timestamp']

        df = pd.DataFrame(data)
        res.append(df)
        offset += limit

    LOGGER.info(f">>>> Finished Grabbing trades for {market} at offset={offset} with taker_only={taker_only}")

    if len(res) == 0:
        LOGGER.info(f"number of data frames captured = {len(res)}")
        return None

    return (
        pd.concat(res, ignore_index=True)
        .assign(date=lambda x: pd.to_datetime(x["timestamp"], unit="s", utc=True).dt.tz_convert(NYC))
        .assign(
            size=lambda x: x['size'].astype(float),
            price=lambda x: x['price'].astype(float)
        )
        .sort_values(by="date")
    )


async def grab_trades_for_trader_market(proxy_wallet: str, condition_id: str, trials: int = 1) -> tuple[pd.DataFrame | None, float | None]:
    df_maker = await get_trades(make_async_session, condition_id, wallet=proxy_wallet, taker_only=False, trials=trials)

    if df_maker is None:    # since df_maker returns all trades, difficult to have taker trades if this is None!
        LOGGER.info(f"No trades for condition id: {condition_id} and proxy_wallet: {proxy_wallet}")
        return None, None

    df_taker = await get_trades(make_async_session, condition_id, wallet=proxy_wallet, taker_only=True, trials=trials)
    return label_taker_trades(df_maker, df_taker)


async def get_trader_information_async(proxy_wallet: str) -> dict[str, Any] | None:
    params = {
        "address": proxy_wallet,
    }
    data: dict[str, Any] = await fetch_json(PUBLIC_PROFILE, make_async_session, params=params)
    return data


async def get_market_outcome(proxy_wallet: str, coin: str, start_date: dt.datetime) -> tuple[MarketOutcome, pd.DataFrame] | tuple[None, None]:
    market_dict = await resolve_market_coin_start_time(coin, start_date, session=make_async_session)
    trader_dict = await get_trader_information_async(proxy_wallet)
    market_id: str = market_dict.condition_id
    df_trades, taker_maker_ratio = await grab_trades_for_trader_market(proxy_wallet, market_id)

    if df_trades is None or len(df_trades) == 0:
        return None, None

    user_full_trades = pl.from_pandas(
        df_trades
        [['date', 'side', 'size', 'price', 'outcome', 'name', 'taker_flag', 'transactionHash', 'proxyWallet', 'conditionId', 'asset', 'title', 'slug']]
    )
    up_price = determine_resolution(market_dict)
    pnl: float = calculate_pl(user_full_trades, up_price)
    name: str = trader_dict['name']
    question: str = market_dict.question
    return MarketOutcome(
        start_date,
        proxy_wallet,
        name,
        market_id,
        market_dict.slug,
        question,
        pnl,
        taker_maker_ratio,
        len(df_trades),
        market_dict.outcome_prices[0]
    ), user_full_trades


async def grab_trades_for_trader(proxy_wallet: str, coin: str, start_date: dt.datetime, end_date: dt.datetime) -> pd.DataFrame:
    date = start_date
    res = []
    while True:
        print(f"Fetching data for {date}")
        trade_results = await get_market_outcome(proxy_wallet, coin, start_date)
        if trade_results:
            res.append(trade_results)
        date = date + dt.timedelta(minutes=15)
        if date >= end_date:
            break

    pnl = pd.DataFrame(res)
    return pnl
