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
OUTPUT_PATH = "public/data/eom_signal.json"   # <-- aligned with workflow
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
# 4. BUILD JSON OBJECT (ALWAYS), ONLY ENTER TRADES ON SIGNAL DAY
# ============================================================

def build_json_object(data, signal_dates):
    today = pd.Timestamp.today().normalize()
    oslo = mcal.get_calendar("XOSL")

    # Last date for which we actually have prices
    last_data_date = data.index[-1]

    # Latest prices (always included in JSON)
    latest_prices = data.loc[last_data_date].to_dict()

    # Find this month's 3rd-last trading day
    this_month_signals = [
        d for d in signal_dates
        if d.year == today.year and d.month == today.month
    ]
    signal_date = this_month_signals[0] if this_month_signals else None

    is_signal_day = signal_date is not None and today == signal_date.normalize()

    base_obj = {
        "date_generated": today.strftime("%Y-%m-%d"),
        "tickers": TOP6,
        "data_last_date": last_data_date.strftime("%Y-%m-%d"),
        "latest_prices": latest_prices,
        "signal_date": signal_date.strftime("%Y-%m-%d") if signal_date is not None else None,
        "is_signal_day": bool(is_signal_day),
    }

    # If today is NOT the 3rd last trading day → no trade, but still output data
    if not is_signal_day:
        base_obj["status"] = "no-signal"
        base_obj["message"] = "No positions entered today."
        return base_obj

    # ========================================================
    # If we are on the signal day, build trade entry/exit info
    # ========================================================

    # Exit date: 8th future trading day (or last available if fewer)
    schedule = oslo.schedule(
        start_date=today,
        end_date=today + pd.Timedelta(days=30)
    )
    trading_days = schedule.index

    if len(trading_days) <= 7:
        exit_date = trading_days[-1]
    else:
        exit_date = trading_days[7]

    # IMPORTANT CHANGE:
    # Use the last available data date for entry prices
    # (this avoids failing when today's close is not yet in the Yahoo data)
    entry_date = last_data_date
    entry_prices = data.loc[entry_date].to_dict()

    base_obj.update({
        "status": "signal",
        "entry_date": entry_date.strftime("%Y-%m-%d"),
        "exit_date": exit_date.strftime("%Y-%m-%d"),
        "entry_prices": entry_prices,
    })

    return base_obj


# ============================================================
# 5. SAVE JSON
# ============================================================

def save_json(obj, filename):
    with open(filename, "w") as f:
        json.dump(obj, f, indent=4)


# ============================================================
# 6. MAIN EXECUTION LOGIC
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
    print("===================\n")

    obj = build_json_object(data, signal_dates)
    save_json(obj, OUTPUT_PATH)
    print(f"JSON written to {OUTPUT_PATH} with status: {obj['status']}")


if __name__ == "__main__":
    main()
