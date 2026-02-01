from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import aiohttp

from config import PM_GET_MARKET_FROM_SLUG_URL


@dataclass(frozen=True, slots=True)
class MarketMeta:
    slug: str
    market_id: int
    start_ms: int
    end_ms: int
    yes_asset_id: str
    no_asset_id: str
    outcomes: Tuple[str, str]
    question: str


def _parse_market_api_response(data: Dict[str, Any]) -> MarketMeta:
    # clobTokenIds is a JSON string list in Gamma response (per your ref code)
    clob_token_ids = json.loads(data["clobTokenIds"])
    if not isinstance(clob_token_ids, list) or len(clob_token_ids) < 2:
        raise RuntimeError(f"Bad clobTokenIds: {clob_token_ids!r}")

    outcomes = json.loads(data["outcomes"])
    if not isinstance(outcomes, list) or len(outcomes) < 2:
        # Fall back to generic labels; still workable
        outcomes = ["YES", "NO"]

    # These timestamps are UTC strings like 2026-01-31T12:45:00Z
    end_dt = dt.datetime.strptime(data["endDate"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    start_dt = dt.datetime.strptime(data["eventStartTime"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)

    # Polymarket Up/Down 15m markets:
    # - outcomes ordering in API tends to align with clobTokenIds ordering.
    # We will treat index 0 as YES (Up) and index 1 as NO (Down).
    yes_asset_id = str(clob_token_ids[0])
    no_asset_id = str(clob_token_ids[1])

    return MarketMeta(
        slug=str(data["slug"]),
        market_id=int(data["id"]),
        start_ms=int(start_dt.timestamp() * 1000),
        end_ms=int(end_dt.timestamp() * 1000),
        yes_asset_id=yes_asset_id,
        no_asset_id=no_asset_id,
        outcomes=(str(outcomes[0]), str(outcomes[1])),
        question=str(data.get("question", "")),
    )


async def resolve_market_by_slug(session: aiohttp.ClientSession, slug: str) -> Optional[MarketMeta]:
    """
    Resolve a Polymarket market by slug using Gamma API.
    Returns MarketMeta or None if not found / non-200.
    """
    url = f"{PM_GET_MARKET_FROM_SLUG_URL}/{slug}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None

    try:
        return _parse_market_api_response(data)
    except Exception:
        return None


def build_15m_slug(coin: str, start_time_utc_s: int) -> str:
    """
    The canonical 15m UpDown slug format used by Gamma (per your reference code):
        f"{coin}-updown-15m-{int(start_time.timestamp())}"
    """
    return f"{coin.lower()}-updown-15m-{int(start_time_utc_s)}"
