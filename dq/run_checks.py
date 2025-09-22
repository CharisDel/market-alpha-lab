# dq/run_checks.py
import sys
from pathlib import Path
import duckdb
import pandas as pd

WAREHOUSE = Path("warehouse/market.duckdb")

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

def pick_close_col(cols: list[str]) -> str | None:
    if "close" in cols:
        return "close"
    if "Close" in cols:
        return '"Close"'  # quoted for case-sensitive identifier
    return None

def main():
    con = duckdb.connect(WAREHOUSE.as_posix())

    # Choose source table
    if table_exists(con, "core.fct_prices_daily"):
        src = "core.fct_prices_daily"
    elif table_exists(con, "raw.equity_prices"):
        src = "raw.equity_prices"
    else:
        print("❌ Neither core.fct_prices_daily nor raw.equity_prices exists. Run your daily loader first.")
        sys.exit(1)

    cols = get_columns(con, src)
    ticker_col = pick_ticker_col(cols)
    close_col = pick_close_col(cols)
    has_date = "date" in cols
    has_ret = "ret_1d" in cols

    print(f"Using source table: {src}")
    print(f"Detected columns: {', '.join(cols)}")
    print("---")

    if not has_date:
        print(f"❌ Source table {src} has no 'date' column — cannot run checks.")
        sys.exit(1)

    hard_fail = False

    # 1) Nulls (with breakdown). Allow ret_1d NULL only on the first row per ticker.
    print("\n--- nulls_in_prices (breakdown) ---")
    if ticker_col:
        if not close_col:
            # No close column; just check date/ticker/ret logic
            sql = f"""
                WITH ordered AS (
                  SELECT
                    date,
                    {ticker_col} AS ticker,
                    ret_1d,
                    ROW_NUMBER() OVER (PARTITION BY {ticker_col} ORDER BY date) AS rn
                  FROM {src}
                )
                SELECT
                  SUM(CASE WHEN date   IS NULL THEN 1 ELSE 0 END) AS null_date,
                  SUM(CASE WHEN ticker IS NULL THEN 1 ELSE 0 END) AS null_ticker,
                  CAST(NULL AS BIGINT) AS null_close,
                  SUM(CASE WHEN ret_1d IS NULL AND rn > 1 THEN 1 ELSE 0 END) AS null_ret_1d_excl_first,
                  SUM(CASE WHEN ret_1d IS NULL AND rn = 1 THEN 1 ELSE 0 END) AS null_ret_1d_first_rows
                FROM ordered;
            """
        else:
            sql = f"""
                WITH ordered AS (
                  SELECT
                    date,
                    {ticker_col} AS ticker,
                    {close_col} AS close,
                    ret_1d,
                    ROW_NUMBER() OVER (PARTITION BY {ticker_col} ORDER BY date) AS rn
                  FROM {src}
                )
                SELECT
                  SUM(CASE WHEN date   IS NULL THEN 1 ELSE 0 END) AS null_date,
                  SUM(CASE WHEN ticker IS NULL THEN 1 ELSE 0 END) AS null_ticker,
                  SUM(CASE WHEN close  IS NULL THEN 1 ELSE 0 END) AS null_close,
                  SUM(CASE WHEN ret_1d IS NULL AND rn > 1 THEN 1 ELSE 0 END) AS null_ret_1d_excl_first,
                  SUM(CASE WHEN ret_1d IS NULL AND rn = 1 THEN 1 ELSE 0 END) AS null_ret_1d_first_rows
                FROM ordered;
            """
        breakdown = con.execute(sql).fetch_df()
        print(breakdown.to_string(index=False))

        # Hard fail conditions
        row = breakdown.iloc[0]
        if (row.get("null_date", 0) > 0) or (row.get("null_ticker", 0) > 0) or (row.get("null_close", 0) or 0) > 0 or (row.get("null_ret_1d_excl_first", 0) > 0):
            hard_fail = True
    else:
        # No ticker column: simpler check
        if close_col:
            sql = f"""
                SELECT
                  SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_date,
                  SUM(CASE WHEN {close_col} IS NULL THEN 1 ELSE 0 END) AS null_close
                FROM {src};
            """
        else:
            sql = f"""
                SELECT
                  SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_date
                FROM {src};
            """
        breakdown = con.execute(sql).fetch_df()
        print(breakdown.to_string(index=False))
        row = breakdown.iloc[0]
        if (row.get("null_date", 0) > 0) or (row.get("null_close", 0) or 0) > 0:
            hard_fail = True

    # 2) Duplicate key check (date, ticker) — only if we have ticker
    if ticker_col:
        print("\n--- dupe_keys_prices ---")
        sql_dupes = f"""
            SELECT COUNT(*) AS n_dupes FROM (
                SELECT date, {ticker_col} AS ticker, COUNT(*) AS c
                FROM {src}
                GROUP BY 1,2
                HAVING COUNT(*) > 1
            );
        """
        n_dupes = con.execute(sql_dupes).fetchone()[0]
        print(pd.DataFrame([{"n_dupes": n_dupes}]).to_string(index=False))
        if n_dupes > 0:
            hard_fail = True
    else:
        print("\n--- dupe_keys_prices ---")
        print("⚠️ Skipped (no ticker-like column detected).")

    # 3) Date gaps estimate (informational)
    print("\n--- date_gaps_prices (Mon–Fri only) ---")
    if ticker_col:
        sql_gaps = f"""
            WITH cal AS (
                SELECT d::DATE AS d
                FROM generate_series(
                    (SELECT MIN(date) FROM {src}),
                    (SELECT MAX(date) FROM {src}),
                    INTERVAL 1 DAY
                ) t(d)
                WHERE EXTRACT(ISODOW FROM d) < 6 -- 1..5 = Mon..Fri
            ),
            per_ticker AS (
                SELECT {ticker_col} AS ticker, COUNT(DISTINCT date) AS have_days
                FROM {src}
                WHERE EXTRACT(ISODOW FROM date) < 6
                GROUP BY 1
            ),
            span AS (
                SELECT
                    {ticker_col} AS ticker,
                    MIN(date) AS min_d,
                    MAX(date) AS max_d
                FROM {src}
                GROUP BY 1
            ),
            expected AS (
                SELECT s.ticker,
                    (SELECT COUNT(*) FROM cal WHERE d BETWEEN s.min_d AND s.max_d) AS expected_days
                FROM span s
            )
            SELECT e.ticker, 
                (SELECT min_d FROM span WHERE span.ticker=e.ticker) AS min_d,
                (SELECT max_d FROM span WHERE span.ticker=e.ticker) AS max_d,
                p.have_days,
                e.expected_days - p.have_days AS missing_weekdays_estimate
            FROM expected e
            JOIN per_ticker p USING (ticker)
            ORDER BY missing_weekdays_estimate DESC
            LIMIT 10;
        """
    else:
        sql_gaps = f"""
            WITH cal AS (
                SELECT d::DATE AS d
                FROM generate_series(
                    (SELECT MIN(date) FROM {src}),
                    (SELECT MAX(date) FROM {src}),
                    INTERVAL 1 DAY
                ) t(d)
                WHERE EXTRACT(ISODOW FROM d) < 6
            ),
            span AS (
                SELECT MIN(date) AS min_d, MAX(date) AS max_d FROM {src}
            ),
            have AS (
                SELECT COUNT(DISTINCT date) AS have_days
                FROM {src}
                WHERE EXTRACT(ISODOW FROM date) < 6
            ),
            expected AS (
                SELECT (SELECT COUNT(*) FROM cal WHERE d BETWEEN (SELECT min_d FROM span) AND (SELECT max_d FROM span)) AS expected_days
            )
            SELECT NULL AS ticker, (SELECT min_d FROM span) AS min_d, (SELECT max_d FROM span) AS max_d,
                (SELECT have_days FROM have) AS have_days,
                expected.expected_days - (SELECT have_days FROM have) AS missing_weekdays_estimate
            FROM expected;
        """
    print(con.execute(sql_gaps).fetch_df().to_string(index=False))


    # 4) Feature table nulls (informational)
    print("\n--- nulls_in_features ---")
    if table_exists(con, "core.feat_equity_daily"):
        feat_cols = get_columns(con, "core.feat_equity_daily")
        pieces = []
        if "rsi_14" in feat_cols: pieces.append("SUM(CASE WHEN rsi_14 IS NULL THEN 1 ELSE 0 END) AS null_rsi")
        if "momentum_10d" in feat_cols: pieces.append("SUM(CASE WHEN momentum_10d IS NULL THEN 1 ELSE 0 END) AS null_mom")
        if "vol_21d" in feat_cols: pieces.append("SUM(CASE WHEN vol_21d IS NULL THEN 1 ELSE 0 END) AS null_vol")
        if pieces:
            sql_feat = f"SELECT {', '.join(pieces)} FROM core.feat_equity_daily;"
            print(con.execute(sql_feat).fetch_df().to_string(index=False))
        else:
            print("⚠️ Feature columns not found.")
    else:
        print("ℹ️ Skipped (core.feat_equity_daily not found).")

    # 5) Freshness check (within last 3 business days)
    print("\n--- freshness_check ---")
    max_date = con.execute(f"SELECT MAX(date) FROM {src}").fetchone()[0]
    print(pd.DataFrame([{"max_date": max_date}]).to_string(index=False))

    from datetime import date, timedelta
    today = date.today()

    # count business days difference (Mon-Fri)
    def business_days_between(a, b):
        if a > b: a, b = b, a
        days = 0
        cur = a
        while cur < b:
            if cur.weekday() < 5:  # 0=Mon .. 4=Fri
                days += 1
            cur += timedelta(days=1)
        return days

    bdiff = business_days_between(max_date, today)
    print(pd.DataFrame([{"business_days_since_max_date": bdiff}]).to_string(index=False))

    # fail if older than 3 business days
    if bdiff > 3:
        hard_fail = True

    # Final status
    if hard_fail:
        print("\n❌ HARD FAIL: Found problematic NULLs or duplicates. See breakdown above.")
        sys.exit(1)
    else:
        print("\n✅ Checks passed (or only informational warnings).")
        sys.exit(0)

if __name__ == "__main__":
    main()
