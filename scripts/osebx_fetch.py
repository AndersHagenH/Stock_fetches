# scripts/osebx_fetch.py
# Fetch 1y of daily OSEBX index data from Yahoo's public chart API
# and write to public/data/osebx.json (date + close).

import os, json, time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

CANDIDATES = ["OSEBX.OL", "^OSEBX"]  # try both symbols
RANGE = "1y"
INTERVAL = "1d"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

def yahoo_chart_url(symbol: str) -> str:
    return (
        f"https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{symbol}?range={RANGE}&interval={INTERVAL}&includePrePost=false"
    )

def http_get_json(url: str) -> dict:
    # simple GET with headers + small retry loop
    last_err = None
    for attempt in range(4):
        try:
            req = Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
            with urlopen(req, timeout=20) as resp:
                data = resp.read()
            return json.loads(data.decode("utf-8"))
        except (HTTPError, URLError, json.JSONDecodeError) as e:
            last_err = e
            # brief backoff
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"GET failed for {url}: {last_err}")

def fetch_rows(symbol: str):
    """Return list of {'t': 'YYYY-MM-DD', 'close': float}."""
    url = yahoo_chart_url(symbol)
    raw = http_get_json(url)

    chart = raw.get("chart", {})
    error = chart.get("error")
    if error:
        raise RuntimeError(f"Yahoo returned error for {symbol}: {error}")

    results = chart.get("result") or []
    if not results:
        raise RuntimeError(f"No result section for {symbol}")

    r0 = results[0]
    ts = r0.get("timestamp") or []
    quotes = ((r0.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quotes.get("close") or []

    rows = []
    for t, c in zip(ts, closes):
        if c is None:
            continue
        # timestamps are seconds since epoch (UTC)
        d = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
        rows.append({"t": d, "close": float(c)})

    if not rows:
        raise RuntimeError(f"No valid rows for {symbol}")

    return rows

def main():
    last_err = None
    for sym in CANDIDATES:
        try:
            print(f"Attempting Yahoo chart API for {sym}")
            rows = fetch_rows(sym)
            source = sym
            print(f"Fetched {len(rows)} rows from {sym}")
            break
        except Exception as e:
            print(f"Failed for {sym}: {e}")
            rows = None
            last_err = e

    if rows is None:
        raise RuntimeError(f"Failed to fetch OSEBX data via Yahoo for {CANDIDATES}. Last error: {last_err}")

    out = {
        "ticker": source,
        "as_of": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "rows": rows,
    }

    os.makedirs("public/data", exist_ok=True)
    out_path = "public/data/osebx.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)

    print(f"Wrote {out_path} with {len(rows)} points (source: {source})")

if __name__ == "__main__":
    main()
