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
TRADELOG_PATH = "public/data/fund2_tradelog.csv"  # Trade log (like Fund 1)

INITIAL_NAV = 100_000.0  # starting NAV for fund 2
FEES_NOK = 0.0           # per-trade fees, if any


# ============================================================
# 2. FILE HELPERS: STATE + TRADELOG
# ============================================================

def ensure_dirs():
    """Ensure public/data/ exists."""
    dirname = os.path.dirname(OUTPUT_PATH)
    if dirname and not os.path.isdir(dirname):
        os.makedirs(dirname, exist_ok=True)


def load_state():
    """Load persistent state (NAV, open positions, planned exit date)."""
    ensure_dirs()
    if not os.path.isfile(STATE_PATH):
        state = {
            "initial_nav": INITIAL_NAV,
            "nav": INITIAL_NAV,
            "open_positions": {},         # ticker -> {qty, entry_price, entry_date, stake_nok}
            "planned_exit_date": None,    # "YYYY-MM-DD" or None
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


def ensure_tradelog_exists():
    """Create tradelog with header if missing."""
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


def append_entry_trade(ticker, entry_date, entry_price, qty, stake_nok, fees_nok):
    """Log a BUY trade with empty exit fields."""
    with open(TRADELOG_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            ticker,
            entry_date,
            float(entry_price),
            "",          # ExitDate
            "",          # ExitPrice
            float(qty),
            float(stake_nok),
            float(fees_nok),
            "",          # PL_NOK
            "",          # PL_PCT
            "BUY"
        ])


def close_open_trades(exit_date_str, exit_prices):
    """
    Fill exit information for all open trades in the tradelog.
    exit_prices: dict[ticker] -> exit_price
    """
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

        pl_nok = qty * exit_price - stake
        pl_pct = pl_nok / stake if stake != 0 else 0.0

        df.loc[idx, "ExitDate"] = exit_date_str
        df.loc[idx, "ExitPrice"] = exit_price
        df.loc[idx, "PL_NOK"] = pl_nok
        df.loc[idx, "PL_PCT"] = pl_pct
        df.loc[idx, "Reason"] = "SELL"

    df.to_csv(TRADELOG_PATH, index=False)


# ============================================================
# 3. DOWNLOAD LATEST DATA
# ============================================================

def fetch_data():
    data = yf.download(TOP6, start=START_DATE, auto_adjust=True)["Close"]
    data = data.loc[:, ~data.columns.duplicated()]
    return data


# ============================================================
# 4. FIND 3RD LAST TRADING DAY OF EACH MONTH (OFFICIAL XOSL CALENDAR)
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

        if len(trading_days) >= 1:
            # last trading day of the month
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
# 6. MAIN EXECUTION LOGIC
# ============================================================

def main():
    ensure_dirs()
    ensure_tradelog_exists()

    # --- Load history & state ---
    data = fetch_data()
    state = load_state()
    signal_dates = compute_signal_dates(data)

    today = pd.Timestamp.today().normalize()
    today_str = today.strftime("%Y-%m-%d")

    # Last available price date
    last_data_date = data.index[-1]
    last_data_date_str = last_data_date.strftime("%Y-%m-%d")
    latest_prices = data.loc[last_data_date].to_dict()

    # Find this month's 3rd-last trading day
    this_month_signals = [
        d for d in signal_dates
        if d.year == today.year and d.month == today.month
    ]
    signal_date = this_month_signals[0] if this_month_signals else None
    signal_date_str = signal_date.strftime("%Y-%m-%d") if signal_date is not None else None
    is_signal_day = signal_date is not None and today == signal_date.normalize()

    has_open_positions = bool(state.get("open_positions"))
    planned_exit_date = state.get("planned_exit_date")
    is_exit_day = bool(planned_exit_date) and (planned_exit_date == today_str) and has_open_positions

    # --------------------------------------------------------
    # EXIT LOGIC (close positions on planned exit date)
    # --------------------------------------------------------
    exit_prices_dict = None

    if is_exit_day:
        exit_prices_dict = {}
        nav_new = 0.0

        for t, pos in state["open_positions"].items():
            if t not in latest_prices:
                continue
            exit_price = float(latest_prices[t])
            exit_prices_dict[t] = exit_price
            qty = float(pos["qty"])
            nav_new += qty * exit_price

        # Update tradelog rows with exit info & PnL
        close_open_trades(exit_date_str=today_str, exit_prices=exit_prices_dict)

        # Update NAV and clear positions
        state["nav"] = float(nav_new)
        state["open_positions"] = {}
        state["planned_exit_date"] = None

        print(f"Closed positions on {today_str}; new NAV: {state['nav']:.2f}")

    # --------------------------------------------------------
    # ENTRY LOGIC (equal-weight NAV on signal day, no open pos)
    # --------------------------------------------------------
    if is_signal_day and not state["open_positions"]:
        # Compute exit date (8th future trading day or last available)
        oslo = mcal.get_calendar("XOSL")
        schedule = oslo.schedule(
            start_date=today,
            end_date=today + pd.Timedelta(days=30)
        )
        trading_days = schedule.index

        # exit on the 7th trading day after the last day of the month
        if len(trading_days) > 7:
            exit_date = trading_days[7]
        else:
            exit_date = trading_days[-1]


        exit_date_str = exit_date.strftime("%Y-%m-%d")

        # Use current NAV; if invalid, reset to initial NAV
        nav_current = float(state.get("nav", 0.0))
        if not np.isfinite(nav_current) or nav_current <= 0:
            nav_current = float(state.get("initial_nav", INITIAL_NAV))
            state["nav"] = nav_current

        stake_per = nav_current / len(TOP6)

        # Use last available data date for entry prices
        entry_date = last_data_date
        entry_date_str = last_data_date_str
        entry_prices_all = data.loc[entry_date].to_dict()

        open_positions = {}

        for t in TOP6:
            price = float(entry_prices_all.get(t, np.nan))
            if not np.isfinite(price) or price <= 0:
                continue

            qty = stake_per / price

            open_positions[t] = {
                "qty": qty,
                "entry_price": price,
                "entry_date": entry_date_str,
                "stake_nok": stake_per
            }

            append_entry_trade(
                ticker=t,
                entry_date=entry_date_str,
                entry_price=price,
                qty=qty,
                stake_nok=stake_per,
                fees_nok=FEES_NOK
            )

        if open_positions:
            state["open_positions"] = open_positions
            state["planned_exit_date"] = exit_date_str
            state["last_signal_date"] = signal_date_str
            # NAV becomes value of open positions at entry
            state["nav"] = float(sum(
                pos["qty"] * pos["entry_price"] for pos in open_positions.values()
            ))

            print(f"Opened equal-weight positions on {entry_date_str}; NAV: {state['nav']:.2f}")
            print(f"Planned exit date: {exit_date_str}")

    # Save updated state
    save_state(state)

    # --------------------------------------------------------
    # BUILD JSON OBJECT FOR FRONTEND
    # --------------------------------------------------------
    base_obj = {
        "date_generated": today_str,
        "tickers": TOP6,
        "data_last_date": last_data_date_str,
        "latest_prices": latest_prices,
        "signal_date": signal_date_str,
        "is_signal_day": bool(is_signal_day),
        "nav": float(state.get("nav", 0.0))
    }

    if state["open_positions"]:
        # Open positions exist: include entry info
        sample_pos = next(iter(state["open_positions"].values()))
        entry_date_str = sample_pos["entry_date"]
        entry_prices_output = {
            t: float(pos["entry_price"]) for t, pos in state["open_positions"].items()
        }

        base_obj.update({
            "status": "signal-open",
            "entry_date": entry_date_str,
            "exit_date": state.get("planned_exit_date"),
            "entry_prices": entry_prices_output
        })
    elif is_exit_day and exit_prices_dict:
        # Positions were just closed today; include exit prices only
        base_obj.update({
            "status": "signal-closed",
            "entry_date": None,
            "exit_date": today_str,
            "exit_prices": {
                t: float(px) for t, px in exit_prices_dict.items()
            }
        })
    else:
        base_obj.setdefault("status", "no-signal")
        base_obj.setdefault("message", "No positions entered today.")

    save_json(base_obj, OUTPUT_PATH)
    print(f"JSON written to {OUTPUT_PATH} with status: {base_obj['status']}")


if __name__ == "__main__":
    main()
