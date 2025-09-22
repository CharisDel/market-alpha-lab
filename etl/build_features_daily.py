# etl/build_features_daily.py
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path

WAREHOUSE = Path("warehouse/market.duckdb")
FEATURE_TABLE = "core.feat_equity_daily"

# ---- helpers ----
def rsi_wilder_14(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    # initial simple means
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    # Wilder smoothing continuation
    avg_gain = avg_gain.combine_first(
        gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    )
    avg_loss = avg_loss.combine_first(
        loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    )

    # avoid divide-by-zero
    denom = avg_loss.replace(0, np.nan)
    rs = avg_gain / denom
    rsi = 100 - (100 / (1 + rs))
    return rsi

def compute_features(df: pd.DataFrame, momentum_lookback: int = 10, vol_window: int = 21) -> pd.DataFrame:
    # Work only with the needed columns, in case extra columns exist
    df = df[["date", "ticker", "close", "ret_1d"]].sort_values(["ticker", "date"]).copy()

    def per_ticker(g: pd.DataFrame) -> pd.DataFrame:
        # g *won't* include the grouping columns when include_groups=False
        g = g.copy()
        g["rsi_14"] = rsi_wilder_14(g["close"], 14)
        g[f"momentum_{momentum_lookback}d"] = (g["close"] / g["close"].shift(momentum_lookback)) - 1.0
        g["vol_21d"] = g["ret_1d"].rolling(vol_window, min_periods=vol_window).std()
        return g

    try:
        # Newer pandas (2.2+): exclude grouping cols during apply, then reattach ticker explicitly
        out = (
            df.groupby("ticker", group_keys=False)
              .apply(lambda g: per_ticker(g).assign(ticker=g.name), include_groups=False)
              .reset_index(drop=True)
        )
    except TypeError:
        # Fallback for older pandas that doesn't have include_groups=
        out = (
            df.groupby("ticker", group_keys=False)
              .apply(lambda g: per_ticker(g.drop(columns=["ticker"])).assign(ticker=g.name))
              .reset_index(drop=True)
        )

    return out[["date", "ticker", "rsi_14", f"momentum_{momentum_lookback}d", "vol_21d"]]

def ensure_schema_and_table(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS core;")
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {FEATURE_TABLE} (
        date DATE,
        ticker VARCHAR,
        rsi_14 DOUBLE,
        momentum_10d DOUBLE,
        vol_21d DOUBLE,
        PRIMARY KEY (date, ticker)
    );
    """)

def table_exists(con, full_name: str) -> bool:
    sch, tbl = full_name.split(".", 1) if "." in full_name else ("main", full_name)
    return con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?;
    """, [sch, tbl]).fetchone()[0] > 0

def get_columns(con, full_name: str) -> list[str]:
    sch, tbl = full_name.split(".", 1) if "." in full_name else ("main", full_name)
    rows = con.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position;
    """, [sch, tbl]).fetchall()
    return [r[0] for r in rows]

def pick_ticker_col(cols: list[str]) -> str | None:
    for c in ["ticker", "symbol", "Ticker", "SYMBOL"]:
        if c in cols:
            return c
    return None

def load_prices_df(con) -> pd.DataFrame:
    """
    Load (date, ticker, close, ret_1d) robustly from core.fct_prices_daily or raw.equity_prices.
    Compute ret_1d if missing.
    """
    if table_exists(con, "core.fct_prices_daily"):
        source = "core.fct_prices_daily"
    elif table_exists(con, "raw.equity_prices"):
        source = "raw.equity_prices"
    else:
        raise RuntimeError("Neither core.fct_prices_daily nor raw.equity_prices exists. Run your daily loader first.")

    cols = get_columns(con, source)
    if "date" not in cols:
        raise RuntimeError(f"'{source}' is missing a 'date' column.")

    # normalize close name if capitalized
    if "close" not in cols and "Close" in cols:
        con.execute(f'CREATE OR REPLACE TEMP VIEW _source_norm AS SELECT *, "Close" AS close FROM {source}')
        source = "_source_norm"
        cols = get_columns(con, source)

    ticker_col = pick_ticker_col(cols)
    if ticker_col is None:
        df = con.execute(f"SELECT date, 'UNKNOWN'::VARCHAR AS ticker, close FROM {source} ORDER BY date").fetch_df()
    else:
        df = con.execute(f"SELECT date, {ticker_col}::VARCHAR AS ticker, close FROM {source} ORDER BY ticker, date").fetch_df()

    if "ret_1d" in cols and ticker_col is not None:
        df_ret = con.execute(f"SELECT date, {ticker_col}::VARCHAR AS ticker, ret_1d FROM {source} ORDER BY ticker, date").fetch_df()
        df = df.merge(df_ret, on=["date", "ticker"], how="left")
    else:
        df["ret_1d"] = df.groupby("ticker")["close"].pct_change()

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["ticker"] = df["ticker"].astype(str)
    return df.sort_values(["ticker", "date"])

def main():
    con = duckdb.connect(WAREHOUSE.as_posix())

    # 1) Load normalized price data
    prices = load_prices_df(con)

    # 2) Compute features in pandas
    feat = compute_features(prices, momentum_lookback=10, vol_window=21)

    # 3) Ensure destination table
    ensure_schema_and_table(con)

    # 4) Register DataFrame and stage → MERGE (this fixes the DuckDB error)
    con.register("feat_df", feat)
    con.execute("CREATE OR REPLACE TEMP TABLE _stg_feat AS SELECT * FROM feat_df")
    con.unregister("feat_df")

    con.execute(f"""
    MERGE INTO {FEATURE_TABLE} AS t
    USING _stg_feat AS s
    ON t.date = s.date AND t.ticker = s.ticker
    WHEN MATCHED THEN UPDATE SET
        rsi_14 = s.rsi_14,
        momentum_10d = s.momentum_10d,
        vol_21d = s.vol_21d
    WHEN NOT MATCHED THEN INSERT (date, ticker, rsi_14, momentum_10d, vol_21d)
    VALUES (s.date, s.ticker, s.rsi_14, s.momentum_10d, s.vol_21d);
    """)

    # 5) quick peek
    sample = con.execute(f"""
        SELECT * FROM {FEATURE_TABLE}
        ORDER BY date DESC, ticker
        LIMIT 10
    """).fetch_df()
    print(sample)

if __name__ == "__main__":
    main()
