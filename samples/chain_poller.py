#!/usr/bin/env python3
"""
polymarket_chain_listener.py

Poll Polygon for Polymarket CTFExchange OrderFilled logs involving a target wallet.
Prints each fill as JSON lines with:
- tx_hash, block_number, log_index
- role (MAKER/TAKER), side (BUY/SELL)
- token_id (outcome ERC-1155 positionId), size (tokens), usdc (USDC), price (usdc/size)
- maker/taker addresses, maker/taker asset ids & amounts (scaled)

Modes:
  --mode ankr     : uses the configured RPC (default: Ankr) and a wider block span.
  --mode polygon  : forces single-block getLogs (fromBlock == toBlock) to avoid -32062 on polygon-rpc.com.

Notes:
- Placements/cancels are NOT on-chain; we only observe fills here.
- Maker/taker is explicit from the OrderFilled event topics (indexed maker/taker).
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Set, Tuple
from dotenv import load_dotenv

import requests


# ----------------------------
# Defaults / constants
# ----------------------------
load_dotenv(override=True)
ANKR_API_KEY = os.getenv('ANKR_API_KEY')
DEFAULT_ANKR_RPC = f"https://rpc.ankr.com/polygon/{ANKR_API_KEY}"
DEFAULT_POLYGON_RPC = "https://polygon-rpc.com"

CTF_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower()

USDC_DECIMALS = 6
TOK_DECIMALS = 6  # Polymarket fill units commonly 1e6 for outcome tokens

# Event signature we keccak:
# OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)
ORDERFILLED_SIG = "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"


# ----------------------------
# Types
# ----------------------------

@dataclass
class FillPrint:
    ts_seen_ms: int
    block_number: int
    tx_hash: str
    log_index: int

    exchange: str

    maker: str
    taker: str

    role: str          # MAKER | TAKER
    side: str          # BUY | SELL

    token_id: int      # outcome token id (positionId)
    size: float        # outcome tokens (scaled)
    usdc: float        # USDC (scaled)
    price: Optional[float]  # usdc / size

    maker_asset_id: int
    taker_asset_id: int
    maker_amount: float
    taker_amount: float
    fee: float


# ----------------------------
# Keccak helper (no web3 dependency)
# ----------------------------

def keccak256_text(s: str) -> str:
    """
    Returns 0x-prefixed keccak256 hash of UTF-8 text.
    Tries pycryptodome first; falls back to eth-hash.
    """
    try:
        from Crypto.Hash import keccak  # type: ignore
        k = keccak.new(digest_bits=256)
        k.update(s.encode("utf-8"))
        return "0x" + k.hexdigest()
    except Exception:
        pass

    try:
        from eth_hash.auto import keccak as eth_keccak  # type: ignore
        return "0x" + eth_keccak(s.encode("utf-8")).hex()
    except Exception as e:
        raise RuntimeError(
            "No keccak implementation found. Install one:\n"
            "  pip install pycryptodome\n"
            "or\n"
            "  pip install eth-hash"
        ) from e


# ----------------------------
# RPC plumbing
# ----------------------------

def rpc(rpc_url: str, method: str, params: List[Any], timeout: float = 20.0) -> Dict[str, Any]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = requests.post(rpc_url, json=payload, headers={"Content-Type": "application/json"}, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"RPC error {method}: {data['error']}")
    return data


def eth_block_number(rpc_url: str) -> int:
    out = rpc(rpc_url, "eth_blockNumber", [])
    return int(out["result"], 16)


def eth_get_logs(rpc_url: str, filt: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = rpc(rpc_url, "eth_getLogs", [filt])
    return out.get("result", []) or []


# ----------------------------
# Hex decoding helpers
# ----------------------------

def topic_to_address(topic_hex: str) -> str:
    hx = topic_hex.lower()
    if hx.startswith("0x"):
        hx = hx[2:]
    return ("0x" + hx[-40:]).lower()


def pad_topic_addr(addr: str) -> str:
    a = addr.lower()
    if a.startswith("0x"):
        a = a[2:]
    return "0x" + ("0" * 24) + a  # 12 bytes padding


def data_words(data_hex: str) -> List[int]:
    d = data_hex[2:] if data_hex.startswith("0x") else data_hex
    if len(d) % 64 != 0:
        d = d.ljust((len(d) + 63) // 64 * 64, "0")
    return [int(d[i:i + 64], 16) for i in range(0, len(d), 64)]


# ----------------------------
# Core decode logic
# ----------------------------

def decode_order_filled_log(log: Dict[str, Any]) -> Tuple[str, str, int, int, int, int, int]:
    """
    Returns:
      maker, taker, makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee

    Assumption (matches observed Polymarket decoding):
      topics = [sig, orderHash, maker, taker]
      data   = [makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee]
    """
    topics = [t.lower() for t in (log.get("topics") or [])]
    if len(topics) < 4:
        raise ValueError("Unexpected topics length for OrderFilled")
    maker = topic_to_address(topics[2])
    taker = topic_to_address(topics[3])

    w = data_words(log.get("data") or "0x")
    if len(w) < 5:
        raise ValueError("Unexpected data length for OrderFilled")
    maker_asset_id = w[0]
    taker_asset_id = w[1]
    maker_amt = w[2]
    taker_amt = w[3]
    fee = w[4]
    return maker, taker, maker_asset_id, taker_asset_id, maker_amt, taker_amt, fee

def classify_fill_for_wallet(
    wallet: str,
    maker: str,
    taker: str,
    maker_asset_id: int,
    taker_asset_id: int,
    maker_amt_raw: int,
    taker_amt_raw: int,
    fee_raw: int,
) -> Tuple[str, str, int, float, float, Optional[float], float, float, float]:
    """
    Returns:
      role, side, token_id, size_tokens, usdc, price, maker_amount_scaled, taker_amount_scaled, fee_scaled
    """
    w = wallet.lower()
    if w == maker:
        role = "MAKER"
        side = "BUY" if maker_asset_id == 0 else "SELL"
    elif w == taker:
        role = "TAKER"
        side = "SELL" if maker_asset_id == 0 else "BUY"
    else:
        raise ValueError("Wallet not maker nor taker for this fill")

    # USDC is represented as assetId == 0 in OrderFilled on Polymarket.
    if maker_asset_id == 0 and taker_asset_id != 0:
        token_id = taker_asset_id
        token_amt_raw = taker_amt_raw
        usdc_amt_raw = maker_amt_raw
    elif taker_asset_id == 0 and maker_asset_id != 0:
        token_id = maker_asset_id
        token_amt_raw = maker_amt_raw
        usdc_amt_raw = taker_amt_raw
    else:
        # Edge paths (merge/redeem); deterministic fallback
        token_id = maker_asset_id if maker_asset_id != 0 else taker_asset_id
        token_amt_raw = maker_amt_raw if maker_asset_id != 0 else taker_amt_raw
        usdc_amt_raw = taker_amt_raw if maker_asset_id != 0 else maker_amt_raw

    size = token_amt_raw / (10 ** TOK_DECIMALS)
    usdc = usdc_amt_raw / (10 ** USDC_DECIMALS)

    price = (usdc / size) if (size > 0 and usdc > 0) else None

    maker_amount_scaled = maker_amt_raw / (10 ** USDC_DECIMALS) if maker_asset_id == 0 else maker_amt_raw / (10 ** TOK_DECIMALS)
    taker_amount_scaled = taker_amt_raw / (10 ** USDC_DECIMALS) if taker_asset_id == 0 else taker_amt_raw / (10 ** TOK_DECIMALS)
    fee_scaled = fee_raw / (10 ** USDC_DECIMALS)

    return role, side, token_id, size, usdc, price, maker_amount_scaled, taker_amount_scaled, fee_scaled


# ----------------------------
# Poll loop
# ----------------------------

def poll_forever(
    rpc_url: str,
    mode: str,
    wallet: str,
    start_block: Optional[int],
    poll_s: float,
    max_block_span: int,
    start_lookback_blocks: int,
) -> None:
    wallet = wallet.lower()
    exchange = CTF_EXCHANGE.lower()

    topic0 = keccak256_text(ORDERFILLED_SIG).lower()

    # Cursor init
    head = eth_block_number(rpc_url)
    if start_block is None:
        # For polygon mode, force a small lookback to reduce immediate load.
        lookback = 3 if mode == "polygon" else start_lookback_blocks
        from_block = max(0, head - lookback)
    else:
        from_block = start_block

    # Dedupe
    seen: Set[Tuple[str, int]] = set()  # (tx_hash, log_index)

    maker_topic = pad_topic_addr(wallet)
    taker_topic = pad_topic_addr(wallet)

    print(json.dumps({
        "type": "listener_start",
        "mode": mode,
        "rpc": rpc_url,
        "exchange": exchange,
        "wallet": wallet,
        "topic0": topic0,
        "from_block": from_block,
        "head": head,
        "poll_s": poll_s,
        "max_block_span": (1 if mode == "polygon" else max_block_span),
    }))

    while True:
        try:
            head = eth_block_number(rpc_url)
            if from_block > head:
                time.sleep(poll_s)
                continue

            # Key behavior switch:
            # - polygon mode: single-block getLogs only (avoids -32062 on polygon-rpc.com)
            # - ankr mode: normal span
            if mode == "polygon":
                to_block = from_block
            else:
                to_block = min(head, from_block + max_block_span)

            # Two queries: wallet as maker OR wallet as taker
            filt_maker = {
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "address": exchange,
                "topics": [topic0, None, maker_topic, None],
            }
            filt_taker = {
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "address": exchange,
                "topics": [topic0, None, None, taker_topic],
            }

            try:
                logs = eth_get_logs(rpc_url, filt_maker) + eth_get_logs(rpc_url, filt_taker)
            except RuntimeError as e:
                msg = str(e)
                # In polygon mode, treat -32062 as "skip this block" to prevent deadlock.
                if mode == "polygon" and ("-32062" in msg or "Block range is too large" in msg):
                    print(json.dumps({"type": "skip_block_due_to_rpc_limit", "block": from_block}))
                    from_block += 1
                    time.sleep(0.05)
                    continue
                raise

            logs.sort(key=lambda x: (int(x["blockNumber"], 16), int(x["logIndex"], 16)))

            now_ms = int(time.time() * 1000)

            for lg in logs:
                txh = (lg.get("transactionHash") or "").lower()
                li = int(lg.get("logIndex") or "0x0", 16)
                if not txh:
                    continue
                key = (txh, li)
                if key in seen:
                    continue
                seen.add(key)

                maker, taker, maker_aid, taker_aid, maker_amt, taker_amt, fee = decode_order_filled_log(lg)

                # Safety: ensure involvement
                if wallet != maker and wallet != taker:
                    continue

                role, side, token_id, size, usdc, price, maker_amount_scaled, taker_amount_scaled, fee_scaled = classify_fill_for_wallet(
                    wallet, maker, taker, maker_aid, taker_aid, maker_amt, taker_amt, fee
                )

                out = FillPrint(
                    ts_seen_ms=now_ms,
                    block_number=int(lg["blockNumber"], 16),
                    tx_hash=txh,
                    log_index=li,
                    exchange=exchange,
                    maker=maker,
                    taker=taker,
                    role=role,
                    side=side,
                    token_id=token_id,
                    size=size,
                    usdc=usdc,
                    price=price,
                    maker_asset_id=maker_aid,
                    taker_asset_id=taker_aid,
                    maker_amount=maker_amount_scaled,
                    taker_amount=taker_amount_scaled,
                    fee=fee_scaled,
                )

                print(json.dumps(asdict(out), separators=(",", ":"), sort_keys=False))

            # Advance cursor
            from_block = to_block + 1

        except Exception as e:
            print(json.dumps({"type": "error", "err": str(e), "from_block": from_block}))
            time.sleep(min(2.0 * poll_s, 15.0))

        time.sleep(poll_s)


# ----------------------------
# CLI
# ----------------------------

def main() -> None:
    # ap = argparse.ArgumentParser(description="Polymarket CTFExchange fill listener (maker/taker, token_id, size, price).")
    # ap.add_argument("--wallet", required=True, help="Trader wallet address (0x...) to watch")
    # ap.add_argument("--mode", choices=["ankr", "polygon"], default="ankr",
    #                 help="ankr = wider spans; polygon = single-block getLogs for polygon-rpc.com")
    # ap.add_argument("--rpc", default=None, help="Override RPC URL (default depends on --mode)")
    # ap.add_argument("--start-block", type=int, default=None, help="Optional start block (default: head-lookback)")
    # ap.add_argument("--lookback", type=int, default=50, help="If start-block not set, start at head-lookback (default 50)")
    # ap.add_argument("--poll-s", type=float, default=5.0, help="Polling interval seconds (default 5)")
    # ap.add_argument("--max-block-span", type=int, default=500, help="Max blocks per eth_getLogs query (ankr mode; default 500)")
    # args = ap.parse_args()

    args = argparse.Namespace(
        wallet="0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d",
        # wallet="0x912a58103662ebe2e30328a305bc33131eca0f92",
        # mode="polygon",  # "ankr" is the other
        mode="ankr",
        rpc=None,
        start_block=None,
        poll_s=5.0,
        lookback=50,
        max_block_span=500,
    )

    if args.rpc is None:
        rpc_url = DEFAULT_ANKR_RPC if args.mode == "ankr" else DEFAULT_POLYGON_RPC
    else:
        rpc_url = args.rpc

    poll_forever(
        rpc_url=rpc_url,
        mode=args.mode,
        wallet=args.wallet,
        start_block=args.start_block,
        poll_s=args.poll_s,
        max_block_span=args.max_block_span,
        start_lookback_blocks=args.lookback,
    )


if __name__ == "__main__":
    main()




