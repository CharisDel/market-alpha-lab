# apps/dashboard.py
import duckdb
import pandas as pd
import streamlit as st
from pathlib import Path

WAREHOUSE = Path("warehouse/market.duckdb")

def table_exists(con, full_name: str) -> bool:
    sch, tbl = full_name.split(".", 1) if "." in full_name else ("main", full_name)
    return con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?;
    """, [sch, tbl]).fetchone()[0] > 0

def get_columns(con, full_name: str) -> list[str]:
    sch, tbl = full_name.split(".", 1) if "." in full_name else ("main", full_name)
    return [r[0] for r in con.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position;
    """, [sch, tbl]).fetchall()]

def pick_ticker_col(cols: list[str]) -> str | None:
    for c in ["ticker", "symbol", "Ticker", "SYMBOL"]:
        if c in cols:
            return c
    return None

def pick_close_col(cols: list[str]) -> str | None:
    if "close" in cols:
        return "close"
    if "Close" in cols:
        return '"Close"'  # quoted for case-sensitive name
    return None

@st.cache_data(ttl=300)
def load_data():
    con = duckdb.connect(WAREHOUSE.as_posix(), read_only=True)

    # Pick a source table
    if table_exists(con, "core.fct_prices_daily"):
        src = "core.fct_prices_daily"
    elif table_exists(con, "raw.equity_prices"):
        src = "raw.equity_prices"
    else:
        raise RuntimeError("No prices table found. Run the daily loader first.")

    cols = get_columns(con, src)
    tcol = pick_ticker_col(cols)
    ccol = pick_close_col(cols)
    has_ret = "ret_1d" in cols
    if tcol is None or ccol is None:
        raise RuntimeError(f"Missing required columns in {src}. Found: {cols}")

    latest_date = con.execute(f"SELECT MAX(date) FROM {src}").fetchone()[0]

    # Latest prices
    if has_ret:
        latest_prices = con.execute(f"""
            SELECT date, {tcol}::VARCHAR AS ticker, {ccol} AS close, ret_1d
            FROM {src}
            WHERE date = (SELECT MAX(date) FROM {src})
            ORDER BY ticker
        """).fetch_df()
    else:
        latest_prices = con.execute(f"""
            WITH w AS (
              SELECT
                date,
                {tcol}::VARCHAR AS ticker,
                {ccol} AS close,
                ({ccol} / LAG({ccol}) OVER (PARTITION BY {tcol} ORDER BY date) - 1) AS ret_1d
              FROM {src}
            )
            SELECT * FROM w
            WHERE date = (SELECT MAX(date) FROM w)
            ORDER BY ticker
        """).fetch_df()

    # Features (if present)
    feat_exists = table_exists(con, "core.feat_equity_daily")
    if feat_exists:
        features = con.execute("""
            SELECT f.date, f.ticker, f.rsi_14, f.momentum_10d, f.vol_21d
            FROM core.feat_equity_daily f
            WHERE f.date = (SELECT MAX(date) FROM core.feat_equity_daily)
            ORDER BY f.ticker
        """).fetch_df()
    else:
        features = pd.DataFrame(columns=["date","ticker","rsi_14","momentum_10d","vol_21d"])

    # History joined with features
    history = con.execute(f"""
        WITH p AS (
          SELECT
            date,
            {tcol}::VARCHAR AS ticker,
            {ccol} AS close,
            {"ret_1d" if has_ret else f"({ccol} / LAG({ccol}) OVER (PARTITION BY {tcol} ORDER BY date) - 1) AS ret_1d"}
          FROM {src}
        )
        SELECT p.date, p.ticker, p.close, p.ret_1d,
               feat.rsi_14, feat.momentum_10d, feat.vol_21d
        FROM p
        LEFT JOIN core.feat_equity_daily AS feat
          ON feat.date = p.date AND feat.ticker = p.ticker
        ORDER BY p.ticker, p.date
    """).fetch_df()

    tickers = sorted(history["ticker"].unique().tolist())
    con.close()
    return latest_date, latest_prices, features, history, tickers, feat_exists

st.set_page_config(page_title="Equities Snapshot", layout="wide")
st.title("📈 Equities Snapshot")

try:
    latest_date, latest_prices, features, history, tickers, feat_exists = load_data()
except Exception as e:
    st.error(f"Failed to load data: {e}")
    st.stop()

st.caption(f"Latest date in warehouse: **{latest_date}**")

left, right = st.columns(2)
with left:
    st.subheader("Latest Prices & Returns")
    st.dataframe(latest_prices, width=True)
with right:
    st.subheader("Latest Features" + ("" if feat_exists else " (table not found)"))
    st.dataframe(features, width=True)

st.markdown("---")
if not tickers:
    st.info("No tickers found in history.")
else:
    sel = st.selectbox("Pick a ticker for history & features:", tickers)
    hist = history[history["ticker"] == sel].sort_values("date")
    st.line_chart(hist.set_index("date")["close"])
    st.line_chart(hist.set_index("date")["ret_1d"])
    feat_cols = [c for c in ["rsi_14","momentum_10d","vol_21d"] if c in hist.columns]
    if feat_cols:
        st.line_chart(hist.set_index("date")[feat_cols])
