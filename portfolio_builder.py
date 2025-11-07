# portfolio_builder.py
# Build a simple portfolio from signals produced by scan_3day.json.
# Rules:
# - Start cash: 50,000 NOK (on first run)
# - Fees: 29 NOK on BUY and 29 NOK on SELL
# - Position size: 5,000 NOK per BUY (skip if cash < 5,029)
# - Max concurrent positions: 10
# - Fractional shares allowed
# - Transact at the LastPrice in scan_3day.json for that day
# - SELL closes position, books P&L, appends to trade_log.csv
# - Writes/updates portfolio_nav.json with daily NAV

from __future__ import annotations
import os
import json
import pandas as pd
from datetime import datetime, timezone

# ====== CONFIG ======
OUTPUT_DIR = "public/data"
SCAN_JSON = os.path.join(OUTPUT_DIR, "scan_3day.json")
STATE_JSON = os.path.join(OUTPUT_DIR, "portfolio_state.json")
TRADE_LOG_CSV = os.path.join(OUTPUT_DIR, "trade_log.csv")
PORTFOLIO_NAV_JSON = os.path.join(OUTPUT_DIR, "portfolio_nav.json")
# NEW: header summary used by the UI to render NAV and P&L (%) boxes
PORTFOLIO_SUMMARY_JSON = os.path.join(OUTPUT_DIR, "portfolio_summary.json")

START_NAV_NOK = 50_000.0
STAKE_NOK = 5_000.0
FEE_BUY = 29.0
FEE_SELL = 29.0
MAX_POSITIONS = 10

# ====== IO HELPERS ======
def _today_date(scan_rows) -> str:
    try:
        dates = {r.get("Date") for r in scan_rows if r.get("Date")}
        if len(dates) == 1:
            d = list(dates)[0]
            return pd.to_datetime(d).strftime("%Y-%m-%d")
    except Exception:
        pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def load_scan():
    if not os.path.exists(SCAN_JSON):
        raise FileNotFoundError(f"Missing {SCAN_JSON}. Run the signal script first.")
    with open(SCAN_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    rows = []
    for r in data:
        t = r.get("Ticker")
        p = r.get("LastPrice")
        s = r.get("Signal")  # CHANGED from "Status"
        if not t or p is None or s is None:
            continue
        rows.append({
            "Ticker": str(t),
            "Status": str(s).upper(),  # CHANGED: normalize Signal to Status uppercase
            "LastPrice": float(p),
            "Date": r.get("Date"),
            "ExitReason": r.get("ExitReason")
        })
    return rows

def load_state():
    if not os.path.exists(STATE_JSON):
        state = {
            "cash": START_NAV_NOK,
            "positions": {},
            "max_slots": MAX_POSITIONS,
            "stake_nok": STAKE_NOK,
            "fee_buy": FEE_BUY,
            "fee_sell": FEE_SELL,
            "start_nav": START_NAV_NOK
        }
        save_state(state)
        return state
    with open(STATE_JSON, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(STATE_JSON, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

def read_trade_log() -> pd.DataFrame:
    if os.path.exists(TRADE_LOG_CSV):
        return pd.read_csv(TRADE_LOG_CSV, dtype={"Ticker": str})
    cols = ["Ticker","EntryDate","EntryPrice","ExitDate","ExitPrice","Qty",
            "StakeNOK","FeesNOK","PL_NOK","PL_PCT","Reason"]
    return pd.DataFrame(columns=cols)

def write_trade_log(df: pd.DataFrame):
    df.to_csv(TRADE_LOG_CSV, index=False)

def read_portfolio_nav() -> pd.DataFrame:
    if os.path.exists(PORTFOLIO_NAV_JSON):
        with open(PORTFOLIO_NAV_JSON, "r", encoding="utf-8") as f:
            arr = json.load(f)
        if isinstance(arr, list) and arr:
            return pd.DataFrame(arr)
    return pd.DataFrame(columns=["date","nav"])

def write_portfolio_nav(df: pd.DataFrame):
    df = df.sort_values("date")
    with open(PORTFOLIO_NAV_JSON, "w", encoding="utf-8") as f:
        json.dump([{"date": d, "nav": float(n)} for d, n in df[["date","nav"]].values], f, indent=2)

# NEW: write compact summary for header boxes
def write_portfolio_summary(date: str, nav: float, start_nav: float):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pl_nok = nav - start_nav
    pl_pct = (pl_nok / start_nav) if start_nav else 0.0
    payload = {
        "date": date,
        "nav": float(nav),
        "pl_nok": float(pl_nok),
        "pl_pct": float(pl_pct)  # e.g., 0.0123 -> 1.23%
    }
    with open(PORTFOLIO_SUMMARY_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

# ====== CORE ======
def process_signals():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    scan = load_scan()
    state = load_state()
    trade_log = read_trade_log()

    today = _today_date(scan)

    by_ticker = {r["Ticker"]: r for r in scan}

    # 1) SELL
    sell_candidates = [t for t, r in by_ticker.items() if r["Status"] == "SELL" and t in state["positions"]]
    for ticker in sorted(sell_candidates):
        pos = state["positions"][ticker]
        last_price = by_ticker[ticker]["LastPrice"]
        qty = float(pos["qty"])
        proceeds = qty * last_price
        state["cash"] += proceeds - FEE_SELL

        stake = float(pos["stake_nok"])
        fees_total = FEE_BUY + FEE_SELL
        pl_nok = (proceeds - stake) - fees_total
        pl_pct = pl_nok / stake if stake != 0 else 0.0
        reason = by_ticker[ticker].get("ExitReason") or "SELL"

        new_row = pd.DataFrame([{
            "Ticker": ticker,
            "EntryDate": pos["entry_date"],
            "EntryPrice": float(pos["entry_price"]),
            "ExitDate": today,
            "ExitPrice": float(last_price),
            "Qty": qty,
            "StakeNOK": stake,
            "FeesNOK": fees_total,
            "PL_NOK": pl_nok,
            "PL_PCT": pl_pct,
            "Reason": reason
        }])
        trade_log = pd.concat([trade_log, new_row], ignore_index=True)

        del state["positions"][ticker]

    if not trade_log.empty:
        for c in ["EntryDate","ExitDate"]:
            trade_log[c] = pd.to_datetime(trade_log[c]).dt.strftime("%Y-%m-%d")
        trade_log = trade_log.drop_duplicates(subset=["Ticker","EntryDate","ExitDate"], keep="first")
        trade_log = trade_log.sort_values(["Ticker","EntryDate","ExitDate"]).reset_index(drop=True)

    # 2) BUY
    current_slots = len(state["positions"])
    free_slots = max(0, state["max_slots"] - current_slots)
    buy_candidates = [t for t, r in by_ticker.items() if r["Status"] == "BUY" and t not in state["positions"]]
    buy_candidates = sorted(buy_candidates)

    for ticker in buy_candidates:
        if free_slots <= 0:
            break
        if state["cash"] < (STAKE_NOK + FEE_BUY):
            continue
        last_price = by_ticker[ticker]["LastPrice"]
        if last_price <= 0:
            continue
        qty = STAKE_NOK / last_price

        state["cash"] -= (STAKE_NOK + FEE_BUY)
        state["positions"][ticker] = {
            "qty": float(qty),
            "entry_price": float(last_price),
            "entry_date": today,
            "stake_nok": float(STAKE_NOK)
        }
        free_slots -= 1

    # 3) NAV
    nav = float(state["cash"])
    for ticker, pos in state["positions"].items():
        px = by_ticker.get(ticker, {}).get("LastPrice", pos["entry_price"])
        nav += float(pos["qty"]) * float(px)

    # 4) NAV log
    nav_df = read_portfolio_nav()
    if nav_df.empty:
        nav_df = pd.DataFrame([{"date": today, "nav": nav}])
    else:
        nav_df = nav_df[nav_df["date"] != today]
        nav_df = pd.concat([nav_df, pd.DataFrame([{"date": today, "nav": nav}])], ignore_index=True)

    # 5) Save
    save_state(state)
    write_trade_log(trade_log)
    write_portfolio_nav(nav_df)

    # NEW: write header summary (NAV + P&L vs start)
    write_portfolio_summary(today, nav, state["start_nav"])

    # Console output (unchanged)
    print(f"[{today}] NAV: {nav:,.2f} NOK | Cash: {state['cash']:,.2f} NOK | Positions: {len(state['positions'])}")
    if state["positions"]:
        print(" Open positions:")
        for t, p in sorted(state["positions"].items()):
            print(f"  - {t}: qty={p['qty']:.6f}, entry={p['entry_price']:.4f} ({p['entry_date']})")

if __name__ == "__main__":
    process_signals()
