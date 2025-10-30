# scripts/osebx_fetch.py
# Fetch 1y of daily OSEBX index data from Yahoo and write to public/data/osebx.json

import os
import json
import datetime as dt


def fetch_series(ticker: str):
    """
    Return [{"t": "YYYY-MM-DD", "close": float}, ...] for the given Yahoo ticker.
    """
    import yfinance as yf

    # Use download() to avoid some Ticker() quirks on indexes
    df = yf.download(
        tickers=ticker,
        period="1y",
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df is None or len(df) == 0:
        raise RuntimeError(f"No data for {ticker}")

    # Accept either 'Close' (download) or 'Adj Close' in odd cases
    close_col = "Close" if "Close" in df.columns else "Adj Close"
    if close_col not in df.columns:
        raise RuntimeError(f"Expected Close/Adj Close column missing for {ticker}")

    df = df.reset_index()[["Date", close_col]]
    rows = [
        {"t": d.strftime("%Y-%m-%d"), "close": float(c)}
        for d, c in zip(df["Date"], df[close_col])
        if c == c  # drop NaNs
    ]
    if not rows:
        raise RuntimeError(f"Only empty/NaN rows for {ticker}")

    return rows


def main():
    # Try common Yahoo symbols for the Oslo Børs Benchmark Index.
    # OSEBX.OL works locally sometimes; ^OSEBX tends to work better in CI.
    candidates = ["OSEBX.OL", "^OSEBX", "^OSEAX"]

    rows = None
    source = None
    last_err = None

    for sym in candidates:
        try:
            print(f"Attempting to fetch: {sym}")
            rows = fetch_series(sym)
            source = sym
            print(f"Fetched {len(rows)} points from {sym}")
            break
        except Exception as e:
            print(f"Failed to fetch {sym}: {e}")
            last_err = e

    if rows is None:
        raise RuntimeError(f"Failed to fetch OSEBX data from all candidates: {candidates}. Last error: {last_err}")

    out = {
        "ticker": source,
        "as_of": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "rows": rows,
    }

    os.makedirs("public/data", exist_ok=True)
    out_path = "public/data/osebx.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)

    print(f"Wrote {out_path} with {len(rows)} points (source: {source})")


if __name__ == "__main__":
    main()
