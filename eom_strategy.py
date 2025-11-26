import pandas as pd
import numpy as np
import yfinance as yf
import json
from datetime import datetime

# ============================================================
# 1. CONFIGURATION
# ============================================================

TOP6 = ["DNO.OL", "CADLR.OL", "SOMA.OL", "AUTO.OL", "BWE.OL", "VAR.OL"]
OUTPUT_PATH = "public/data/eom_strategy.json"

START_DATE = "2010-01-01"  # for building trade calendar


# ============================================================
# 2. DOWNLOAD LATEST DATA
# ============================================================

def fetch_data():
    data = yf.download(TOP6, start=START_DATE, auto_adjust=True)["Close"]
    data = data.loc[:, ~data.columns.duplicated()]
    return data


# ============================================================
# 3. FIND 3RD LAST TRADING DAY OF EACH MONTH
# ============================================================

def compute_signal_dates(data):
    signal_dates = []

    for (year, month), group in data.groupby([data.index.year, data.index.month]):
        if len(group) >= 3:
            signal_dates.append(group.index[-3])  # 3rd last day

    return pd.DatetimeIndex(signal_dates)


# ============================================================
# 4. CHECK IF TODAY IS A SIGNAL DAY
# ============================================================

def today_is_signal_day(signal_dates):
    today = pd.Timestamp.today().normalize()

    # running on GitHub, time may be UTC — normalize
    return today in signal_dates


# ============================================================
# 5. GET ENTRY PRICE AND CALCULATE EXIT DATE
# ============================================================

def build_trade_signal(data, signal_dates):
    today = pd.Timestamp.today().normalize()

    entry_idx = data.index.get_loc(today)
    exit_idx = entry_idx + 7  # 7 trading days later

    if exit_idx >= len(data):
        return None  # out of bounds

    entry_prices = data.loc[today].to_dict()
    exit_date = data.index[exit_idx]

    return:
        "date_generated": today.strftime("%Y-%m-%d"),
        "entry_date": today.strftime("%Y-%m-%d"),
        "exit_date": exit_date.strftime("%Y-%m-%d"),
        "tickers": TOP6,
        "entry_prices": entry_prices,
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

    if not today_is_signal_day(signal_dates):
        print("Not a signal day. Exiting silently.")
        return

    signal = build_trade_signal(data, signal_dates)

    if signal is None:
        print("Could not generate signal (out of data range).")
        return

    save_json(signal, OUTPUT_PATH)
    print(f"Signal generated and saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
