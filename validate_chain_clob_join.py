import json


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def pct(x, n):
    return 0.0 if n <= 0 else 100.0 * x / n


def quantile(xs, q):
    xs = sorted(xs)
    if not xs:
        return None
    i = int(q * (len(xs) - 1))
    return xs[i]


def main():
    ltp_path = "logs/run_.../polymarket_last_trade_price.jsonl"
    match_path = "chain_clob_match.debug.jsonl"

    # tx_hash -> ts_clob_ms (latest)
    clob = {}
    for r in read_jsonl(ltp_path):
        # crash-fast required fields
        for k in ["tx_hash", "ts_clob_ms"]:
            if k not in r:
                raise KeyError(f"{ltp_path} missing {k}")
        clob[r["tx_hash"]] = int(r["ts_clob_ms"])

    matched = 0
    total = 0
    lags = []

    for r in read_jsonl(match_path):
        if r.get("type") != "chain_clob_match":
            continue
        for k in ["tx_hash", "ts_chain_ms", "matched", "use_ms", "lag_est_ms"]:
            if k not in r:
                raise KeyError(f"{match_path} missing {k}")

        total += 1
        if r["matched"]:
            matched += 1
            if r.get("lag_ms") is not None:
                lags.append(int(r["lag_ms"]))

    print(f"match: {matched}/{total} ({pct(matched, total):.2f}%)")
    if lags:
        print("lag_ms:")
        print("  n    :", len(lags))
        print("  p10  :", quantile(lags, 0.10))
        print("  p50  :", quantile(lags, 0.50))
        print("  p90  :", quantile(lags, 0.90))
        print("  min  :", min(lags))
        print("  max  :", max(lags))


if __name__ == "__main__":
    main()
