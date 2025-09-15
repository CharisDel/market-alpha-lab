from datetime import date, timedelta
import os
import pandas as pd
import yfinance as yf

# You can override tickers via environment variable: TICKERS="SPY,AAPL,MSFT"
TICKERS = os.getenv("TICKERS", "SPY,AAPL,MSFT").split(",")

def flatten_cols(cols):
    """Flatten possible MultiIndex columns into simple strings."""
    flat = []
    for c in cols:
        if isinstance(c, tuple):
            parts = [str(x) for x in c if x is not None and str(x) != ""]
            flat.append("_".join(parts))
        else:
            flat.append(str(c))
    return flat

def pick_col(df, base, ticker):
    """
    Find a column for a given base name (e.g., 'open') across different yfinance variants,
    such as 'open', 'Open', 'open_SPY', 'SPY_Open', etc.
    """
    cols = [c.lower() for c in df.columns]
    colmap = dict(zip(cols, df.columns))  # lower->original

    base = base.lower().replace(" ", "_")
    t = ticker.lower()

    candidates = [
        base,
        base.replace(" ", "_"),
        f"{base}_{t}",
        f"{t}_{base}",
    ]

    # also handle variants like 'adj close'
    if base == "adj_close":
        candidates.extend(["adj close", "adjclose"])

    # direct matches first
    for cand in candidates:
        cand_l = cand.replace(" ", "_")
        if cand_l in colmap:
            return colmap[cand_l]

    # fallback: any column that starts with base_
    for c in cols:
        if c.startswith(base + "_"):
            return colmap[c]

    return None

end = date.today()
start = end - timedelta(days=120)

os.makedirs("data/raw", exist_ok=True)
out_path = os.path.join("data", "raw", f"equity_prices_{end.strftime('%Y%m%d')}.csv")

rows = []
for t in TICKERS:
    t = t.strip()
    df = yf.download(
        t,
        start=start,
        end=end + timedelta(days=1),
        interval="1d",
        auto_adjust=True,     # adjusted prices (no 'Adj Close' column)
        progress=False,
        group_by="column",    # helps avoid nested columns, but we'll normalize anyway
    )

    if not isinstance(df, pd.DataFrame) or df.empty:
        print(f"⚠️ No data for {t}")
        continue

    df = df.reset_index()
    df.columns = flatten_cols(df.columns)
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # Find the right columns regardless of suffix/prefix
    c_open   = pick_col(df, "open", t)
    c_high   = pick_col(df, "high", t)
    c_low    = pick_col(df, "low", t)
    c_close  = pick_col(df, "close", t)
    c_volume = pick_col(df, "volume", t)

    needed = {"open": c_open, "high": c_high, "low": c_low, "close": c_close, "volume": c_volume}
    missing = [k for k, v in needed.items() if v is None]
    if missing or "date" not in df.columns:
        print(f"⚠️ Missing columns for {t}: {missing or []}. Got cols={list(df.columns)}")
        continue

    df = df.rename(columns={
        c_open: "open",
        c_high: "high",
        c_low: "low",
        c_close: "close",
        c_volume: "volume",
    })

    df["symbol"] = t.upper()
    df = df[["date", "open", "high", "low", "close", "volume", "symbol"]]
    rows.append(df)

if rows:
    out = pd.concat(rows, ignore_index=True)
    out.sort_values(["symbol", "date"], inplace=True)
    out.to_csv(out_path, index=False)
    print(f"✅ Saved {len(out)} rows → {out_path}")
else:
    print("❌ No data fetched. Check internet connection or tickers.")
