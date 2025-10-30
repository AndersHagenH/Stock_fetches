import json, datetime as dt

def fetch_series(ticker: str):
    import yfinance as yf
    df = yf.Ticker(ticker).history(period="1y", interval="1d", auto_adjust=False)
    if df.empty:
        raise RuntimeError(f"No data for {ticker}")
    df = df.reset_index()[["Date", "Close"]]
    rows = [{"t": d.strftime("%Y-%m-%d"), "close": float(c)} for d, c in zip(df["Date"], df["Close"])]
    return rows

def main():
    # OSEBX is usually ^OSEBX on Yahoo; ^OSEAX as fallback.
    rows = None
    last_err = None
    for sym in ["^OSEBX", "^OSEAX"]:
        try:
            rows = fetch_series(sym)
            source = sym
            break
        except Exception as e:
            last_err = e
    if rows is None:
        raise last_err

    out = {
        "ticker": source,
        "as_of": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "rows": rows,
    }

    # Write to your Pages data folder
    with open("public/data/osebx.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)

    print(f"Wrote public/data/osebx.json with {len(rows)} points (source: {source})")

if __name__ == "__main__":
    main()

