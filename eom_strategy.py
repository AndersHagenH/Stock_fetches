import pandas as pd
import numpy as np
import yfinance as yf
import json
from datetime import datetime
import pandas_market_calendars as mcal

# ============================================================
# 1. CONFIGURATION
# ============================================================

TOP6 = ["DNO.OL", "CADLR.OL", "SOMA.OL", "AUTO.OL", "BWE.OL", "VAR.OL"]
OUTPUT_PATH = "public/data/eom_strategy.json"
START_DATE = "2010-01-01"


# ============================================================
# 2. DOWNLOAD LATEST DATA
# ============================================================

def fetch_data():
    data = yf.download(TOP6, start=START_DATE, auto_adjust=True)["Close"]
    data = data.loc[:, ~data.columns.duplicated()]
    return data


# ============================================================
# 3. FIND 3RD LAST TRADING DAY OF EACH MONTH (OFFICIAL XOSL CALENDAR)
# ============================================================

def compute_signal_dates(data):
    oslo = mcal.get_calendar("XOSL")
    signal_dates = []

    all_dates = data.index
    years_months = sorted(set((d.year, d.month) for d in all_dates))

    for year, month in years_months:
        start = pd.Timestamp(year=year, month=month, day=1)
        end = start + pd.offsets.MonthEnd(1)

        schedule = oslo.schedule(start_date=start, end_date=end)
        trading_days = schedule.index

        if len(trading_days) >= 3:
            signal_dates.append(trading_days[-3])

    return pd.DatetimeIndex(signal_dates)


# ============================================================
# 4. CHECK IF TODAY IS A SIGNAL DAY
# ============================================================

def today_is_signal_day(signal_dates):
    today = pd.Timestamp.today().normalize()
    return today in signal_dates


# ============================================================
# 5. GET ENTRY PRICE & EXIT DATE (USING OFFICIAL CALENDAR)
# ============================================================

def build_trade_signal(data, signal_dates):
    today = pd.Timestamp.today().normalize()

    oslo = mcal.get_calendar("XOSL")

    schedule = oslo.schedule(
        start_date=today,
        end_date=today + pd.Timedelta(days=30)
    )
    trading_days = schedule.index

    # Fallback logic: if fewer than 8 future trading days exist
    if len(trading_days) <= 7:
        exit_date = trading_days[-1]  # fallback to last available trading day
    else:
        exit_date = trading_days[7]

    entry_prices = data.loc[today].to_dict()

    return {
        "status": "signal",
        "date_generated": today.strftime("%Y-%m-%d"),
        "entry_date": today.strftime("%Y-%m-%d"),
        "exit_date": exit_date.strftime("%Y-%m-%d"),
        "tickers": TOP6,
        "entry_prices": entry_prices
    }


# ============================================================
# 6. SAVE JSON
# ============================================================

def save_json(obj, filename):
    with open(filename, "w") as f:
        json.dump(obj, f, indent=4)


# ============================================================
# 7. MAIN EXECUTION LOGIC
# ============================================================

def main():
    data = fetch_data()
    signal_dates = compute_signal_dates(data)
    today = pd.Timestamp.today().normalize()

    print("=== DEBUG INFO ===")
    print("Today interpreted as:", today)
    print("Last date in dataset:", data.index[-1])
    print("Is today in dataset?", today in data.index)
    print()

    print("Last date per ticker:")
    for t in TOP6:
        print(f"{t}: {data[t].dropna().index[-1]}")
    print()

    print("Signal dates this month:")
    print([d for d in signal_dates if d.month == today.month and d.year == today.year])
    print("Today is signal day?", today_is_signal_day(signal_dates))
    print("===================\n")

    # Always produce JSON
    if not today_is_signal_day(signal_dates):
        no_signal_obj = {
            "status": "no-signal",
            "date_generated": today.strftime("%Y-%m-%d"),
            "message": "No positions entered today."
        }
        save_json(no_signal_obj, OUTPUT_PATH)
        print("No positions entered.")
        return

    # Signal day → attempt to produce a signal
    try:
        signal = build_trade_signal(data, signal_dates)
        save_json(signal, OUTPUT_PATH)
        print(f"Signal generated and saved to {OUTPUT_PATH}")
    except Exception as e:
        fallback_obj = {
            "status": "no-signal",
            "date_generated": today.strftime("%Y-%m-%d"),
            "message": f"Signal day identified, but error occurred: {str(e)}"
        }
        save_json(fallback_obj, OUTPUT_PATH)
        print("Signal day, but failed to generate signal. Fallback JSON saved.")


if __name__ == "__main__":
    main()
