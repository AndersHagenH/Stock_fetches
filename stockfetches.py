# Stock_fetches.py
# Fetches Oslo stocks from Yahoo Finance and generates BUY/SELL/CAUTION/HOLD/IN_POSITION signals
# Strategy (summary):
# - Buy: If on day t any of the 1–3 day returns is in [-4%, -3%], and day t+1 closes up vs t, BUY at close of t+1.
# - While in position (one per ticker):
#     * SELL if close >= entry * 1.05 (target, priority).
#     * Else if price has been above entry after entry day and later close < entry, SELL (break-even).
#     * Else if today close < yesterday close: flag "Caution!" (keep holding).
# - Daily output: latest-day status per ticker.

from __future__ import annotations
from datetime import datetime
from typing import Optional, List, Dict, Any
import os
import json
import numpy as np
import pandas as pd
import yfinance as yf

# ===== PARAMETERS =====
START_DATE = "2024-01-01"        # earliest date to fetch
END_DATE: Optional[str] = None    # None = today
USE_ADJUSTED = False              # True = adjusted close
BAND_LOW = -0.04                  # -4%
BAND_HIGH = -0.03                 # -3% (inclusive)
TARGET_PCT = 0.05                 # +5% profit target
LOOKBACK_SET = (1, 2, 3)          # rolling window lengths to test
# =======================

# List of tickers (Oslo Stock Exchange)
TICKERS = [
    "PROT.OL", "GJF.OL", "STB.OL", "ORK.OL", "EPR.OL", "KID.OL",
    "DNB.OL", "SB1NO.OL", "SBNOR.OL", "MING.OL", "NONG.OL",
    "MORG.OL", "VEI.OL", "AFG.OL"
]

# ===== Create output folder =====
OUTPUT_DIR = "public/data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_prices(tickers: List[str], start: str, end: Optional[str], use_adjusted: bool) -> pd.DataFrame:
    print("Fetching data from Yahoo Finance...")
    data = yf.download(tickers=tickers, start=start, end=end, progress=False, auto_adjust=False)
    if isinstance(data.columns, pd.MultiIndex):
        px = data["Adj Close" if use_adjusted else "Close"].copy()
    else:
        px = data["Adj Close" if use_adjusted else "Close"].to_frame(tickers[0])
    px = px.sort_index()
    px = px.dropna(how="all")
    return px

def _status_row(ticker: str, open_trade: Dict[str, Any] | None, date, status: str, last_price: float,
                entry_price: float | None = None, exit_reason: str | None = None) -> Dict[str, Any]:
    entry_p = entry_price if entry_price is not None else (open_trade["entry_price"] if open_trade else None)
    entry_d = (open_trade["entry_date"] if open_trade else None)
    target_p = (entry_p * (1 + TARGET_PCT)) if entry_p is not None else None
    ret_since_entry = (last_price / entry_p - 1.0) if (entry_p is not None and last_price and entry_p) else None
    return {
        "Ticker": ticker,
        "Date": pd.Timestamp(date).strftime("%Y-%m-%d"),
        "TimeUTC": datetime.utcnow().strftime("%H:%M UTC"),
        "Status": status,  # BUY / SELL / CAUTION / IN_POSITION / HOLD
        "LastPrice": round(float(last_price), 6) if pd.notna(last_price) else None,
        "EntryDate": pd.Timestamp(entry_d).strftime("%Y-%m-%d") if entry_d is not None else None,
        "EntryPrice": round(float(entry_p), 6) if entry_p is not None else None,
        "TargetPrice": round(float(target_p), 6) if target_p is not None else None,
        "ReturnSinceEntry": round(float(ret_since_entry), 6) if ret_since_entry is not None else None,
        "ExitReason": exit_reason,
        "Note": "Caution!" if status == "CAUTION" else None,
    }

def find_trades_for_series(close: pd.Series) -> Dict[str, Any]:
    s = close.dropna()
    if s.empty:
        today = None
    else:
        today = s.index[-1]

    # Precompute rolling returns R^{k}_t for k in {1,2,3}
    rets_by_k = {k: s.pct_change(k) for k in LOOKBACK_SET}

    dates = s.index.to_list()
    open_trade: Dict[str, Any] | None = None
    today_status = None

    def is_band_day(idx: int) -> bool:
        if idx <= 0:
            return False
        t = dates[idx]
        for k in LOOKBACK_SET:
            if idx - k < 0:
                continue
            r = rets_by_k[k].loc[t]
            if pd.notna(r) and (BAND_LOW <= r <= BAND_HIGH):
                return True
        return False

    i = 0
    n = len(dates)
    while i < n:
        d = dates[i]
        price = s.loc[d]

        if open_trade is None:
            # Buy if yesterday was a band day and today reverses up
            if i - 1 >= 0 and is_band_day(i - 1):
                if price > s.loc[dates[i - 1]]:
                    open_trade = {
                        "entry_date": d,
                        "entry_price": float(price),
                        "armed_break_even": False,
                    }
                    if d == today:
                        today_status = _status_row(close.name, open_trade, d, "BUY", price)
                    i += 1
                    continue
            if d == today and today_status is None:
                today_status = _status_row(close.name, None, d, "HOLD", price)

        else:
            entry = open_trade["entry_price"]

            # Arm break-even once we have any close > entry after entry day
            if d > open_trade["entry_date"] and price > entry:
                open_trade["armed_break_even"] = True

            # 1) Target sell (priority)
            if price >= entry * (1 + TARGET_PCT):
                if d == today:
                    today_status = _status_row(close.name, None, d, "SELL", price, entry_price=entry, exit_reason="TARGET")
                open_trade = None
                i += 1
                continue

            # 2) Break-even sell if armed and price now < entry
            if open_trade.get("armed_break_even", False) and price < entry:
                if d == today:
                    today_status = _status_row(close.name, None, d, "SELL", price, entry_price=entry, exit_reason="BREAKEVEN")
                open_trade = None
                i += 1
                continue

            # 3) Otherwise: Caution if down day, else In Position
            if i - 1 >= 0 and price < s.loc[dates[i - 1]]:
                if d == today:
                    today_status = _status_row(close.name, open_trade, d, "CAUTION", price)
            else:
                if d == today:
                    today_status = _status_row(close.name, open_trade, d, "IN_POSITION", price)

        i += 1

    if today_status is None and today is not None:
        if open_trade is not None:
            today_status = _status_row(close.name, open_trade, today, "IN_POSITION", s.loc[today])
        else:
            today_status = _status_row(close.name, None, today, "HOLD", s.loc[today])

    return {"today_status": today_status}

def build_snapshot(price_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for ticker in price_df.columns:
        series = price_df[ticker].dropna()
        res = find_trades_for_series(series)
        rows.append(res["today_status"])
    snapshot_df = pd.DataFrame(rows).sort_values(["Status", "Ticker"])
    return snapshot_df

def main():
    price_data = load_prices(TICKERS, START_DATE, END_DATE, USE_ADJUSTED)
    price_data = price_data.dropna(how="all", axis=1)
    if price_data.empty:
        raise RuntimeError("No price data downloaded; check tickers or date range.")

    snapshot_df = build_snapshot(price_data)

    # ===== Save outputs to the SAME filenames as the previous script =====
    csv_path = os.path.join(OUTPUT_DIR, "scan_3day.csv")
    json_path = os.path.join(OUTPUT_DIR, "scan_3day.json")

    snapshot_df.to_csv(csv_path, index=False)
    snapshot_df.to_json(json_path, orient="records", indent=2, date_format="iso")

    # ===== Console preview =====
    print("\n=== Today’s Signals (latest day per ticker) ===")
    print(snapshot_df.to_string(index=False))
    print(f"\nSaved results to:\n - {csv_path}\n - {json_path}")

if __name__ == "__main__":
    main()
