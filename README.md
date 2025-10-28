# Stock_fetches.py
# Fetches Oslo stocks from Yahoo Finance and generates BUY/HOLD signals

from __future__ import annotations
from datetime import datetime
from typing import Optional
import os
import json
import numpy as np
import pandas as pd
import yfinance as yf

# ===== PARAMETERS =====
START_DATE = "2024-01-01"       # earliest date to fetch
END_DATE: Optional[str] = None   # None = today
LOOKBACK_DAYS = 3                # 3-day return window
DROP_THRESHOLD = -0.03           # BUY if 3-day return < -3%
USE_ADJUSTED = False             # True = adjusted close
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

# ===== Download prices =====
print("Fetching data from Yahoo Finance...")
data = yf.download(
    tickers=TICKERS,
    start=START_DATE,
    end=END_DATE,
    progress=False
)

if USE_ADJUSTED:
    price_data = data["Adj Close"].copy()
else:
    price_data = data["Close"].copy()

price_data.dropna(how="all", inplace=True)

# ===== Calculate returns =====
returns = price_data.pct_change(LOOKBACK_DAYS)
latest_returns = returns.iloc[-1].sort_values()

# ===== Generate signals =====
signals = pd.DataFrame({
    "Ticker": latest_returns.index,
    "3D_Return": latest_returns.values,
})
signals["Signal"] = np.where(signals["3D_Return"] <= DROP_THRESHOLD, "BUY", "HOLD")
signals["LastPrice"] = price_data.iloc[-1].values
signals["Date"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

# ===== Save outputs =====
csv_path = os.path.join(OUTPUT_DIR, "scan_3day.csv")
json_path = os.path.join(OUTPUT_DIR, "scan_3day.json")

signals.to_csv(csv_path, index=False)
signals.to_json(json_path, orient="records", indent=2)

print(f"\nSaved results to:\n - {csv_path}\n - {json_path}")
print("\nPreview:")
print(signals.head())
