import os
import csv
import json
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf
import pandas_market_calendars as mcal

# ============================================================
# 1. CONFIGURATION
# ============================================================

TOP6 = ["DNO.OL", "CADLR.OL", "SOMA.OL", "AUTO.OL", "BWE.OL", "VAR.OL"]

START_DATE = "2010-01-01"

OUTPUT_PATH = "public/data/eom_signal.json"       # JSON for frontend
STATE_PATH = "public/data/fund2_state.json"       # Persistent NAV + positions
TRADELOG_PATH = "public/data/fund2_tradelog.csv"  # Trade log
NAV_PATH = "public/data/fund2_nav.json"           # NAV time series (NEW)

INITIAL_NAV = 50_000.0
FEE_PER_TRADE = 29.0      # BUY = 29, SELL = 29 NOK


# ============================================================
# 2. FILE HELPERS: STATE + NAV + TRADELOG
# ============================================================

def ensure_dirs():
    dirname = os.path.dirname(OUTPUT_PATH)
    if dirname and not os.path.isdir(dirname):
        os.makedirs(dirname, exist_ok=True)


def load_state():
    ensure_dirs()
    if not os.path.isfile(STATE_PATH):
        state = {
            "initial_nav": INITIAL_NAV,
            "nav": INITIAL_NAV,
            "open_positions": {},
            "planned_exit_date": None,
            "last_signal_date": None
        }
        save_state(state)
        return state

    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    ensure_dirs()
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=4)


def load_nav_history():
    if not os.path.isfile(NAV_PATH):
        return []
    with open(NAV_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_nav_history(history):
    with open(NAV_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=4)


def record_nav(date_str, nav_value):
    """Append or update NAV history."""
    history = load_nav_history()

    if history and history[-1]["date"] == date_str:
        history[-1]["nav"] = nav_value
    else:
        history.append({"date": date_str, "nav": nav_value})

    save_nav_history(history)


def ensure_tradelog_exists():
    ensure_dirs()
    if not os.path.isfile(TRADELOG_PATH):
        with open(TRADELOG_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Ticker", "EntryDate", "EntryPrice",
                "ExitDate", "ExitPrice",
                "Qty", "StakeNOK", "FeesNOK",
                "PL_NOK", "PL_PCT", "Reason"
            ])


def append_entry_trade(ticker, entry_date, entry_price, qty, stake_nok):
    """Store BUY trade — charge 29 NOK per BUY."""
    with open(TRADELOG_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            ticker,
            entry_date,
            float(entry_price),
            "", "",
            float(qty),
            float(stake_nok),
            float(FEE_PER_TRADE),  # BUY fee
            "", "",
            "BUY"
        ])


def close_open_trades(exit_date_str, exit_prices):
    if not os.path.isfile(TRADELOG_PATH):
        return

    df = pd.read_csv(TRADELOG_PATH)

    for idx, row in df[df["ExitDate"] == ""].iterrows():
        ticker = row["Ticker"]
        if ticker not in exit_prices:
            continue

        exit_price = float(exit_prices[ticker])
        entry_price = float(row["EntryPrice"])
        qty = float(row["Qty"])
        stake = float(row["StakeNOK"])

        buy_fee = float(row["FeesNOK"])   # stored earlier
        sell_fee = FEE_PER_TRADE          # new fee
        total_fees = buy_fee + sell_fee

        # Profit = qty * exit - stake - sell_fee
        pl_nok = qty * exit_price - stake - sell_fee
        pl_pct = pl_nok / stake if stake > 0 else 0

        df.loc[idx, "ExitDate"] = exit_date_str
        df.loc[idx, "ExitPrice"] = exit_price
        df.loc[idx, "FeesNOK"] = total_fees
        df.loc[idx, "PL_NOK"] = pl_nok
        df.loc[idx, "PL_PCT"] = pl_pct
        df.loc[idx, "Reason"] = "SELL"

    df.to_csv(TRADELOG_PATH, index=False)


# ============================================================
# 3. DOWNLOAD DATA
# ============================================================

def fetch_data():
    data = yf.download(TOP6, start=START_DATE, auto_adjust=True)["Close"]
    data = data.loc[:, ~data.columns.duplicated()]
    return data


# ============================================================
# 4. SIGNAL = LAST TRADING DAY OF THE MONTH
# ============================================================

def compute_signal_dates(data):
    oslo = mcal.get_calendar("XOSL")
    signal_dates = []

    for year, month in sorted(set((d.year, d.month) for d in data.index)):
        start = pd.Timestamp(year=year, month=month, day=1)
        end = start + pd.offsets.MonthEnd(1)
        schedule = oslo.schedule(start_date=start, end_date=end)
        trading_days = schedule.index
        if len(trading_days) > 0:
            signal_dates.append(trading_days[-1])

    return pd.DatetimeIndex(signal_dates)


# ============================================================
# 5. SAVE JSON
# ============================================================

def save_json(obj, filename):
    ensure_dirs()
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=4)


# ============================================================
# 6. MAIN LOGIC
# ============================================================

def main():
    ensure_dirs()
    ensure_tradelog_exists()

    data = fetch_data()
    state = load_state()
    signal_dates = compute_signal_dates(data)

    today = pd.Timestamp.today().normalize()
    today_str = today.strftime("%Y-%m-%d")

    last_data_date = data.index[-1]
    last_data_date_str = last_data_date.strftime("%Y-%m-%d")
    latest_prices = data.loc[last_data_date].to_dict()

    # Detect signal day
    this_month_signal = [
        d for d in signal_dates if d.year == today.year and d.month == today.month
    ]
    signal_date = this_month_signal[0] if this_month_signal else None
    signal_date_str = signal_date.strftime("%Y-%m-%d") if signal_date else None
    is_signal_day = signal_date is not None and today == signal_date.normalize()

    has_open_positions = bool(state["open_positions"])
    planned_exit_date = state.get("planned_exit_date")
    is_exit_day = (planned_exit_date == today_str) and has_open_positions

    # ============================================================
    # EXIT LOGIC
    # ============================================================
    exit_prices_dict = None

    if is_exit_day:
        exit_prices_dict = {}
        nav_new = 0.0
        total_sell_fees = 0.0

        for t, pos in state["open_positions"].items():
            px = latest_prices[t]
            qty = pos["qty"]

            exit_prices_dict[t] = px
            nav_new += qty * px
            total_sell_fees += FEE_PER_TRADE

        nav_new -= total_sell_fees  # Subtract SELL costs

        close_open_trades(today_str, exit_prices_dict)

        state["nav"] = float(nav_new)
        state["open_positions"] = {}
        state["planned_exit_date"] = None

    # ============================================================
    # ENTRY LOGIC
    # ============================================================

    if is_signal_day and not has_open_positions:
        oslo = mcal.get_calendar("XOSL")
        schedule = oslo.schedule(start_date=today, end_date=today + pd.Timedelta(days=30))
        trading_days = schedule.index

        exit_date = trading_days[7] if len(trading_days) > 7 else trading_days[-1]
        exit_date_str = exit_date.strftime("%Y-%m-%d")

        # Deduct BUY fees
        nav_current = state["nav"]
        total_buy_fees = len(TOP6) * FEE_PER_TRADE
        nav_after_fees = nav_current - total_buy_fees
        state["nav"] = nav_after_fees

        stake_per = nav_after_fees / len(TOP6)

        entry_prices = data.loc[last_data_date].to_dict()
        open_positions = {}

        for t in TOP6:
            px = entry_prices[t]
            qty = stake_per / px

            open_positions[t] = {
                "qty": qty,
                "entry_price": px,
                "entry_date": last_data_date_str,
                "stake_nok": stake_per
            }

            append_entry_trade(
                t,
                last_data_date_str,
                px,
                qty,
                stake_per
            )

        state["open_positions"] = open_positions
        state["planned_exit_date"] = exit_date_str

    # ============================================================
    # DAILY NAV UPDATE
    # ============================================================

    if state["open_positions"]:
        nav_live = sum(
            pos["qty"] * latest_prices[t] for t, pos in state["open_positions"].items()
        )
        state["nav"] = float(nav_live)

    # Save state and NAV series
    save_state(state)
    record_nav(today_str, state["nav"])

    # ============================================================
    # BUILD FRONTEND JSON
    # ============================================================

    obj = {
        "date_generated": today_str,
        "tickers": TOP6,
        "data_last_date": last_data_date_str,
        "latest_prices": latest_prices,
        "signal_date": signal_date_str,
        "is_signal_day": is_signal_day,
        "nav": state["nav"]
    }

    if state["open_positions"]:
        obj.update({
            "status": "signal-open",
            "entry_date": next(iter(state["open_positions"].values()))["entry_date"],
            "exit_date": state["planned_exit_date"],
            "entry_prices": {t: pos["entry_price"] for t, pos in state["open_positions"].items()}
        })
    elif is_exit_day:
        obj.update({
            "status": "signal-closed",
            "exit_date": today_str,
            "exit_prices": exit_prices_dict
        })
    else:
        obj.update({
            "status": "no-signal",
            "message": "No positions entered today."
        })

    save_json(obj, OUTPUT_PATH)
    print(f"JSON updated: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
