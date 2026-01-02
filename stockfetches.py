# Stock_fetches.py
# Fetches Oslo stocks from Yahoo Finance and generates BUY/SELL/CAUTION/HOLD/IN_POSITION signals
# Eksport: Samme output som Kode 1 (Ticker, 3D_Return, Signal, LastPrice, Date)

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
END_DATE: Optional[str] = None   # None = today
USE_ADJUSTED = True             # True = adjusted close

# --- Updated buy/sell thresholds (aligned with your backtest concept) ---
BAND_LOW = -0.03                 # -3%
BAND_HIGH = -0.01                # -1% (inclusive)
TARGET_PCT = 0.04                # +4% profit target

LOOKBACK_SET = (1, 2, 3)         # rolling window lengths to test

# For "Kode 1"-kompatibel eksport:
LOOKBACK_DAYS_EXPORT = 3         # 3-dagers retur publiseres som 3D_Return

# --- Portfolio concept (aligned with backtest) ---
ALLOCATION_PCT = 0.25            # 25% of NAV per position
MAX_POSITIONS_ALLOWED = int(1.0 / ALLOCATION_PCT)  # 4 positions
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

# ---------- Data download (robust) ----------
def load_prices(tickers: List[str], start: str, end: Optional[str], use_adjusted: bool) -> pd.DataFrame:
    print("Fetching data from Yahoo Finance...")
    data = yf.download(
        tickers=tickers,
        start=start,
        end=end,
        progress=False,
        auto_adjust=False,
        group_by="column",      # stabiliserer kolonnerekkefølge
        interval="1d",
        threads=True,
    )
    price_key = "Adj Close" if use_adjusted else "Close"

    if isinstance(data.columns, pd.MultiIndex):
        # Variant A: ('Close', 'DNB.OL', ...)
        if price_key in data.columns.get_level_values(0):
            px = data[price_key].copy()
        # Variant B: ('DNB.OL', 'Close', ...)
        elif price_key in data.columns.get_level_values(1):
            px = data.xs(price_key, axis=1, level=1).copy()
        else:
            raise KeyError(f"Could not find '{price_key}' in downloaded data.")
    else:
        # Én ticker → enkel DataFrame
        if price_key in data.columns:
            px = data[[price_key]].copy()
            px.columns = [tickers[0]]
        else:
            raise KeyError(f"Could not find '{price_key}' in downloaded data (single ticker).")

    px = px.sort_index().dropna(how="all")
    return px

# ---------- Trading logic ----------
def _status_row(
    ticker: str,
    open_trade: Dict[str, Any] | None,
    date,
    status: str,
    last_price: float,
    entry_price: float | None = None,
    exit_reason: str | None = None
) -> Dict[str, Any]:
    TARGET_P = TARGET_PCT
    entry_p = entry_price if entry_price is not None else (open_trade["entry_price"] if open_trade else None)
    entry_d = (open_trade["entry_date"] if open_trade else None)
    target_p = (entry_p * (1 + TARGET_P)) if entry_p is not None else None
    ret_since_entry = (last_price / entry_p - 1.0) if (entry_p is not None and last_price and entry_p) else None
    return {
        "Ticker": ticker,
        "Date": pd.Timestamp(date).strftime("%Y-%m-%d"),
        "TimeUTC": datetime.utcnow().strftime("%H:%M UTC"),
        "Status": status,  # BUY / SELL / CAUTION / IN_POSITION / HOLD
        "LastPrice": float(last_price) if pd.notna(last_price) else None,
        "EntryDate": pd.Timestamp(entry_d).strftime("%Y-%m-%d") if entry_d is not None else None,
        "EntryPrice": float(entry_p) if entry_p is not None else None,
        "TargetPrice": float(target_p) if target_p is not None else None,
        "ReturnSinceEntry": float(ret_since_entry) if ret_since_entry is not None else None,
        "ExitReason": exit_reason,
        "Note": "Caution!" if status == "CAUTION" else None,
    }

def find_trades_for_series(close: pd.Series) -> Dict[str, Any]:
    s = close.dropna()
    if s.empty:
        return {"today_status": None}

    today = s.index[-1]
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
            if i - 1 >= 0 and is_band_day(i - 1) and price > s.loc[dates[i - 1]]:
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

            if d > open_trade["entry_date"] and price > entry:
                open_trade["armed_break_even"] = True

            # 1) Target
            if price >= entry * (1 + TARGET_PCT):
                if d == today:
                    today_status = _status_row(close.name, None, d, "SELL", price, entry_price=entry, exit_reason="TARGET")
                open_trade = None
                i += 1
                continue

            # 2) Break-even
            if open_trade.get("armed_break_even", False) and price < entry:
                if d == today:
                    today_status = _status_row(close.name, None, d, "SELL", price, entry_price=entry, exit_reason="BREAKEVEN")
                open_trade = None
                i += 1
                continue

            # 3) Caution/in position
            if i - 1 >= 0 and price < s.loc[dates[i - 1]]:
                if d == today:
                    today_status = _status_row(close.name, open_trade, d, "CAUTION", price)
            else:
                if d == today:
                    today_status = _status_row(close.name, open_trade, d, "IN_POSITION", price)

        i += 1

    if today_status is None:
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

# Kode 1 forventer Signal som BUY/HOLD; mapp fra Status
def status_to_signal(status: str) -> str:
    if status == "BUY":
        return "BUY"
    if status == "SELL":
        return "SELL"
    return "HOLD"

# ---------- Main ----------
def main():
    # ---- Load price data ----
    price_data = load_prices(TICKERS, START_DATE, END_DATE, USE_ADJUSTED)
    price_data = price_data.dropna(how="all", axis=1)
    if price_data.empty:
        raise RuntimeError("No price data downloaded.")

    snapshot_df = build_snapshot(price_data)

    # ---- Load REAL portfolio state ----
    portfolio_path = os.path.join(OUTPUT_DIR, "portfolio_state.json")
    if os.path.exists(portfolio_path):
        with open(portfolio_path, "r") as f:
            portfolio_state = json.load(f)
        real_positions = portfolio_state.get("positions", {})
        cash = float(portfolio_state.get("cash", 0.0))
    else:
        real_positions = {}   # no positions yet
        cash = 0.0

    # ---- Compute 3-day return ----
    three_day_rets = price_data.pct_change(LOOKBACK_DAYS_EXPORT).iloc[-1]
    three_day_rets.name = "3D_Return"

    # ---- Merge model results and returns ----
    out = snapshot_df.merge(
        three_day_rets,
        left_on="Ticker",
        right_index=True,
        how="left",
    )

    # ---- Compute NAV and investable amount (portfolio concept) ----
    # NAV = cash + sum(shares * last_price) across current holdings.
    # invest_amount = min(cash, 0.25 * NAV)
    last_price_map = dict(zip(out["Ticker"], out["LastPrice"]))

    nav = cash
    for tkr, pos in real_positions.items():
        try:
            shares = float(pos.get("shares", 0.0))
            px = float(last_price_map.get(tkr, np.nan))
            if shares > 0 and pd.notna(px):
                nav += shares * px
        except Exception:
            pass

    target_invest = nav * ALLOCATION_PCT
    invest_amount = min(cash, target_invest)

    # ---- Override signals using REAL portfolio P&L + capacity ----
    for idx, row in out.iterrows():
        ticker = row["Ticker"]
        last_price = row["LastPrice"]

        # If ticker is in portfolio → apply HOLD/SELL rules
        if ticker in real_positions:
            entry_price = float(real_positions[ticker]["entry_price"])
            real_pl_pct = (last_price / entry_price - 1.0) if entry_price > 0 else 0.0

            # Real SELL rule (aligned with updated target)
            if real_pl_pct >= TARGET_PCT:
                out.at[idx, "Status"] = "SELL"
            else:
                out.at[idx, "Status"] = "HOLD"

        # If not in portfolio:
        else:
            # Keep BUY only if:
            #  - model says BUY
            #  - portfolio has capacity (< 4 positions)
            #  - investable cash per rule (min(cash, 0.25*NAV)) is > 0
            if row["Status"] == "BUY":
                if len(real_positions) >= MAX_POSITIONS_ALLOWED:
                    out.at[idx, "Status"] = "HOLD"
                elif invest_amount <= 0:
                    out.at[idx, "Status"] = "HOLD"
                else:
                    continue  # allowed BUY
            else:
                out.at[idx, "Status"] = "HOLD"

    # ---- Convert Status → Signal ----
    out["Signal"] = out["Status"].map(status_to_signal)

    # ---- Build Kode 1 date string ----
    out["Date"] = out["Date"].astype(str) + " " + out["TimeUTC"].astype(str)

    # ---- Final formatting ----
    out["LastPrice"] = out["LastPrice"].round(6)
    out = out[["Ticker", "3D_Return", "Signal", "LastPrice", "Date"]].copy()
    out = out.sort_values("3D_Return")

    # ---- Save JSON + CSV ----
    csv_path = os.path.join(OUTPUT_DIR, "scan_3day.csv")
    json_path = os.path.join(OUTPUT_DIR, "scan_3day.json")

    out.to_csv(csv_path, index=False)
    out.to_json(json_path, orient="records", indent=2)

    print("\n=== Export with REAL portfolio signals ===")
    print(out.head().to_string(index=False))
    print(f"Saved to:\n - {csv_path}\n - {json_path}")

if __name__ == "__main__":
    main()
