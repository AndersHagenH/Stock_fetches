import pandas as pd
import yfinance as yf
import json
from datetime import datetime, timezone

# =============================================================
# CONFIG
# =============================================================
TICKERS = ["DNO.OL", "CADLR.OL", "SOMA.OL", "AUTO.OL", "BWE.OL", "VAR.OL"]
OUTPUT_PATH = "public/data/eom_signal.json"
DAYS_FORWARD = 7  # exit 7 trading days after entry


# =============================================================
# HELPER: FIND 5TH LAST TRADING DAY OF CURRENT MONTH
# =============================================================
def get_fifth_last_trading_day(prices: pd.DataFrame) -> pd.Timestamp:
    """Returns the 5th last trading day of the current month."""
    today = pd.Timestamp.now().tz_localize("UTC").tz_convert("Europe/Oslo").normalize()

    this_year = today.year
    this_month = today.month

    month_data = prices.loc[
        (prices.index.year == this_year) &
        (prices.index.month == this_month)
    ]

    if len(month_data) < 5:
        return None

    # 5th last trading day = index[-5]
    return month_data.index[-5]


# =============================================================
# MAIN SCRIPT
# =============================================================
def main():
    # Fetch data for the last ~3 months (enough to detect month end + exit day)
    data = yf.download(TICKERS, period="6mo", auto_adjust=True)["Close"]

    # Remove duplicates if Yahoo returns any
    data = data.loc[:, ~data.columns.duplicated()]

    # Get today's date in Oslo time
    now = pd.Timestamp.now().tz_localize("UTC").tz_convert("Europe/Oslo")
    today = now.normalize()

    # Determine the 5th last trading day
    fifth_last_td = get_fifth_last_trading_day(data)

    if fifth_last_td is None:
        print("Not enough data to compute 5th last trading day.")
        return

    # ---------------------------------------------------------
    # CHECK IF TODAY IS THE SIGNAL DAY
    # ---------------------------------------------------------
    if today != fifth_last_td:
        print("Not the 5th last trading day. No signal generated.")
        return

    print("Today IS the 5th last trading day. Generating signal...")

    # Entry prices today
    if today not in data.index:
        print("Today's close price not available yet.")
        return

    entry_prices = data.loc[today]

    # Compute exit date = 7 trading days after entry
    today_idx = data.index.get_loc(today)
    exit_idx = today_idx + DAYS_FORWARD

    if exit_idx >= len(data):
        print("Not enough future data to compute exit.")
        exit_date = None
        exit_prices = None
    else:
        exit_date = data.index[exit_idx]
        exit_prices = data.loc[exit_date].to_dict()

    # ---------------------------------------------------------
    # BUILD JSON OUTPUT
    # ---------------------------------------------------------
    output = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "signal_date": today.strftime("%Y-%m-%d"),
        "tickers": TICKERS,
        "entry_prices": entry_prices.to_dict(),
        "exit_date_estimate": exit_date.strftime("%Y-%m-%d") if exit_date else None,
        "exit_prices_estimate": exit_prices,
    }

    # Save to file
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=4)

    print(f"Signal saved to {OUTPUT_PATH}")


# =============================================================
# RUN
# =============================================================
if __name__ == "__main__":
    main()

