# chain_marker_task.py
from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from dotenv import load_dotenv

from plot_ipc import PlotMarker
from state import AppState

load_dotenv(override=True)

# RPCs
ANKR_API_KEY = os.getenv("ANKR_API_KEY", "").strip()
DEFAULT_ANKR_RPC = (
    f"https://rpc.ankr.com/polygon/{ANKR_API_KEY}" if ANKR_API_KEY else "https://rpc.ankr.com/polygon"
)
DEFAULT_POLYGON_RPC = "https://polygon-rpc.com"

# Polymarket CTF Exchange (spender)
CTF_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower()
POLYGON_USDC_ADDR = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".lower()

USDC_DECIMALS = 6
TOK_DECIMALS = 6  # Polymarket outcome token fill units commonly 1e6

ORDERFILLED_SIG = "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
ERC20_TRANSFER_SIG = "Transfer(address,address,uint256)"

_dbg_path = Path("chain_marker.debug.jsonl")
_match_dbg_path = Path("chain_clob_match.debug.jsonl")


def _dbg_log(obj: dict) -> None:
    try:
        with _dbg_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _match_dbg_log(obj: dict) -> None:
    try:
        with _match_dbg_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        pass


# ----------------------------
# keccak helper (no web3)
# ----------------------------
def keccak256_text(s: str) -> str:
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


def eth_get_block_by_number(rpc_url: str, block_number: int) -> Dict[str, Any]:
    # second param=false => no full txs
    out = rpc(rpc_url, "eth_getBlockByNumber", [hex(block_number), False])
    return out.get("result") or {}


def block_ts_s_from_block(rpc_url: str, block_number: int) -> float:
    blk = eth_get_block_by_number(rpc_url, block_number)
    ts_hex = blk.get("timestamp")
    if not ts_hex:
        raise RuntimeError(f"Missing timestamp for block {block_number}")
    return float(int(ts_hex, 16))


def eth_get_tx_receipt(rpc_url: str, tx_hash: str) -> Dict[str, Any]:
    out = rpc(rpc_url, "eth_getTransactionReceipt", [tx_hash])
    return out.get("result") or {}


# ----------------------------
# decode helpers
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
    return "0x" + ("0" * 24) + a


def data_words(data_hex: str) -> List[int]:
    d = data_hex[2:] if data_hex.startswith("0x") else data_hex
    if len(d) % 64 != 0:
        d = d.ljust((len(d) + 63) // 64 * 64, "0")
    return [int(d[i : i + 64], 16) for i in range(0, len(d), 64)]


def decode_order_filled_log(log: Dict[str, Any]) -> Tuple[str, str, int, int, int, int, int]:
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
) -> Tuple[str, str, int, float, float, Optional[float]]:
    """
    PRESERVED behavior:
    Returns: role, side, token_id, size_tokens, usdc, price
    """
    w = wallet.lower()
    if w == maker:
        role = "MAKER"
        side = "BUY" if maker_asset_id == 0 else "SELL"
    elif w == taker:
        role = "TAKER"
        side = "SELL" if maker_asset_id == 0 else "BUY"
    else:
        raise ValueError("Wallet not maker nor taker")

    # USDC is assetId == 0 in Polymarket OrderFilled
    if maker_asset_id == 0 and taker_asset_id != 0:
        token_id = taker_asset_id
        token_amt_raw = taker_amt_raw
        usdc_amt_raw = maker_amt_raw
    elif taker_asset_id == 0 and maker_asset_id != 0:
        token_id = maker_asset_id
        token_amt_raw = maker_amt_raw
        usdc_amt_raw = taker_amt_raw
    else:
        # fallback
        token_id = maker_asset_id if maker_asset_id != 0 else taker_asset_id
        token_amt_raw = maker_amt_raw if maker_asset_id != 0 else taker_amt_raw
        usdc_amt_raw = taker_amt_raw if maker_asset_id != 0 else maker_amt_raw

    size = token_amt_raw / (10**TOK_DECIMALS)
    usdc = usdc_amt_raw / (10**USDC_DECIMALS)
    price = (usdc / size) if (size > 0 and usdc > 0) else None
    return role, side, int(token_id), float(size), float(usdc), (float(price) if price is not None else None)


def _nonblocking_put(q, obj) -> None:
    try:
        q.put_nowait(obj)
    except Exception:
        # drop one and retry
        try:
            _ = q.get_nowait()
        except Exception:
            pass
        try:
            q.put_nowait(obj)
        except Exception:
            pass


def _active_token_ids(state) -> set[int]:
    out = set()
    y = getattr(state.book, "yes_asset_id", "") or ""
    n = getattr(state.book, "no_asset_id", "") or ""
    try:
        if y:
            out.add(int(y))
    except Exception:
        pass
    try:
        if n:
            out.add(int(n))
    except Exception:
        pass
    return out


def _usdc_delta_raw_from_receipt(receipt: Dict[str, Any], wallet: str) -> Optional[int]:
    """
    Signed USDC delta for `wallet` from ERC-20 Transfer logs in the tx receipt.
      negative => wallet paid USDC
      positive => wallet received USDC
    Returns None if receipt/logs missing.
    """
    logs = receipt.get("logs")
    if not isinstance(logs, list):
        return None

    wallet = wallet.lower()
    usdc_addr = POLYGON_USDC_ADDR
    topic0 = keccak256_text(ERC20_TRANSFER_SIG).lower()

    d = 0
    any_hit = False

    for lg in logs:
        try:
            if (lg.get("address") or "").lower() != usdc_addr:
                continue
            topics = [t.lower() for t in (lg.get("topics") or [])]
            if not topics or topics[0] != topic0:
                continue
            if len(topics) < 3:
                continue

            frm = topic_to_address(topics[1])
            to = topic_to_address(topics[2])
            amt = int(lg.get("data") or "0x0", 16)

            if frm == wallet:
                d -= amt
                any_hit = True
            if to == wallet:
                d += amt
                any_hit = True
        except Exception:
            continue

    return d if any_hit else 0


async def chain_marker_task(
    state: AppState,
    plot_q,
    *,
    wallet: str,
    mode: str = "ankr",  # "ankr" | "polygon"
    poll_s: float = 1.0,
    lookback_blocks: int = 50,
    max_block_span: int = 500,
    start_block: Optional[int] = None,
) -> None:
    wallet = wallet.lower()

    rpc_url = DEFAULT_ANKR_RPC if mode == "ankr" else DEFAULT_POLYGON_RPC
    exchange = CTF_EXCHANGE
    topic0 = keccak256_text(ORDERFILLED_SIG).lower()

    # Keep a small, stable distance behind head (avoid querying unstable head).
    FINALITY = 1
    START_WARMUP = 2  # start near-live immediately, not a big backfill

    head = await asyncio.to_thread(eth_block_number, rpc_url)
    if start_block is None:
        # Start close to "safe head" so the dashboard feels near-live immediately.
        from_block = max(0, head - FINALITY - START_WARMUP)
    else:
        from_block = int(start_block)

    seen: Set[Tuple[str, int]] = set()

    # block_number -> ts_s (int seconds since epoch)
    block_ts_cache: Dict[int, float] = {}
    # tx_hash -> receipt (optional cache; keeps load down within a run)
    receipt_cache: Dict[str, Dict[str, Any]] = {}

    maker_topic = pad_topic_addr(wallet)
    taker_topic = pad_topic_addr(wallet)

    while True:
        try:
            head = await asyncio.to_thread(eth_block_number, rpc_url)
            safe_head = max(0, head - FINALITY)

            # If we're caught up to safe_head, wait for more blocks.
            if from_block > safe_head:
                _dbg_log(
                    {
                        "type": "chain_marker_liveness",
                        "ts_ms": int(time.time() * 1000),
                        "rpc_mode": mode,
                        "head": head,
                        "safe_head": safe_head,
                        "from_block": from_block,
                        "behind_blocks": 0,
                    }
                )
                await asyncio.sleep(poll_s)
                continue

            # polygon-rpc.com can be finicky; force single-block
            # For ankr: clamp to safe_head (not head) so we don't chase unstable tip.
            to_block = from_block if mode == "polygon" else min(safe_head, from_block + max_block_span)

            _dbg_log(
                {
                    "type": "chain_marker_liveness",
                    "ts_ms": int(time.time() * 1000),
                    "rpc_mode": mode,
                    "head": head,
                    "safe_head": safe_head,
                    "from_block": from_block,
                    "to_block": to_block,
                    "behind_blocks": max(0, safe_head - from_block),
                }
            )

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

            def _get():
                return eth_get_logs(rpc_url, filt_maker) + eth_get_logs(rpc_url, filt_taker)

            try:
                logs = await asyncio.to_thread(_get)
            except RuntimeError as e:
                msg = str(e)
                if mode == "polygon" and ("-32062" in msg or "Block range is too large" in msg):
                    from_block += 1
                    await asyncio.sleep(0.05)
                    continue
                raise

            logs.sort(key=lambda x: (int(x["blockNumber"], 16), int(x["logIndex"], 16)))

            # Determine current window start for plotting (ms)
            ws_ms = float(getattr(state.plot_ctl, "active_win_start_ms", 0.0) or 0.0)
            if ws_ms <= 0.0:
                ws_ms = float(getattr(state.driver, "win_start_ms", 0.0) or 0.0)

            for lg in logs:
                bn = int(lg["blockNumber"], 16)

                ts_s = block_ts_cache.get(bn)
                if ts_s is None:
                    ts_s = float(await asyncio.to_thread(block_ts_s_from_block, rpc_url, bn))
                    block_ts_cache[bn] = ts_s

                ts_chain_ms = int(float(ts_s) * 1000.0)

                txh = (lg.get("transactionHash") or "").lower()
                li = int(lg.get("logIndex") or "0x0", 16)
                if not txh:
                    continue
                key = (txh, li)
                if key in seen:
                    continue
                seen.add(key)

                maker, taker, maker_aid, taker_aid, maker_amt, taker_amt, fee = decode_order_filled_log(lg)
                if wallet != maker and wallet != taker:
                    continue

                role, side_old, token_id, size, usdc, price = classify_fill_for_wallet(
                    wallet, maker, taker, maker_aid, taker_aid, maker_amt, taker_amt, fee
                )

                if price is None:
                    continue  # for plot we need y

                active = _active_token_ids(state)
                if not active:
                    continue
                if int(token_id) not in active:
                    continue

                # --- side_final from USDC receipt delta (preserves old as fallback) ---
                side_final = side_old
                usdc_delta_raw: Optional[int] = None
                try:
                    rec = receipt_cache.get(txh)
                    if rec is None:
                        rec = await asyncio.to_thread(eth_get_tx_receipt, rpc_url, txh)
                        receipt_cache[txh] = rec

                    usdc_delta_raw = _usdc_delta_raw_from_receipt(rec, wallet)
                    # If we found any USDC movement, use it to define BUY/SELL:
                    # negative => paid USDC => BUY
                    # positive => received USDC => SELL
                    if usdc_delta_raw is not None:
                        if usdc_delta_raw < 0:
                            side_final = "BUY"
                        elif usdc_delta_raw > 0:
                            side_final = "SELL"
                        # else 0: keep side_old (no information)
                except Exception:
                    # keep side_old on any failure
                    pass

                # --- NEW: try exact tx_hash match to CLOB `last_trade_price` for precise timestamp ---
                ts_clob_ms: Optional[int] = None
                lag_ms: Optional[int] = None
                matched: bool = False
                reason: str = ""

                try:
                    p = state.clob_tx.get(txh)
                    if p is not None:
                        ts_clob_ms = int(p.ts_clob_ms)
                        lag_ms = int(ts_clob_ms - ts_chain_ms)
                        state.clob_chain_lag.update(float(lag_ms))
                        matched = True
                    else:
                        reason = "no_clob_print"
                except Exception as e:
                    reason = f"clob_lookup_error:{type(e).__name__}"

                lag_est_ms = float(state.clob_chain_lag.estimate())
                use_ms = int(ts_clob_ms) if matched and ts_clob_ms is not None else int(ts_chain_ms + lag_est_ms)

                # Dedicated match/fallback logging.
                _match_dbg_log(
                    {
                        "type": "chain_clob_match",
                        "ts_local_ms": int(time.time() * 1000),
                        "tx_hash": txh,
                        "block_number": bn,
                        "ts_chain_ms": ts_chain_ms,
                        "ts_clob_ms": ts_clob_ms,
                        "use_ms": use_ms,
                        "matched": matched,
                        "reason": reason,
                        "lag_ms": lag_ms,
                        "lag_est_ms": lag_est_ms,
                        "disp_ms": float(state.clob_chain_lag.dispersion()),
                        "n": int(state.clob_chain_lag.n),
                        "token_id": int(token_id),
                        "role": role,
                        "side": side_final,
                        "size": float(size),
                        "price": float(price),
                    }
                )

                m = PlotMarker(
                    ts_s=float(use_ms) / 1000.0,
                    win_start_ms=float(ws_ms),
                    y=float(price),
                    token_id=int(token_id),
                    role=str(role),
                    side=str(side_final),
                    size=float(size),
                    price=float(price),
                    tx_hash=txh,
                )

                # PRESERVED debug logging, plus minimal extra fields
                _dbg_log(
                    {
                        "type": "orderfilled",
                        "ts_ms": int(time.time() * 1000),
                        "wallet": wallet,
                        "rpc_mode": mode,
                        "block_number": bn,
                        "block_ts_s": ts_s,
                        "tx_hash": txh,
                        "log_index": li,
                        "maker": maker,
                        "taker": taker,
                        "maker_asset_id": int(maker_aid),
                        "taker_asset_id": int(taker_aid),
                        "maker_amt_raw": int(maker_amt),
                        "taker_amt_raw": int(taker_amt),
                        "fee_raw": int(fee),
                        "derived": {
                            "role": role,
                            "side_old": side_old,
                            "side_final": side_final,
                            "token_id": int(token_id),
                            "size": float(size),
                            "usdc": float(usdc),
                            "price": price,
                            "usdc_delta_raw": usdc_delta_raw,
                        },
                        "active_state": {
                            "yes_asset_id": getattr(state.book, "yes_asset_id", None),
                            "no_asset_id": getattr(state.book, "no_asset_id", None),
                        },
                    }
                )

                _nonblocking_put(plot_q, m)

            from_block = to_block + 1

        except Exception:
            await asyncio.sleep(min(2.0 * poll_s, 15.0))

        await asyncio.sleep(poll_s)
