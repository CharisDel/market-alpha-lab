import os, sys
import duckdb

DB_PATH = os.path.join("warehouse", "market.duckdb")
GLOB = os.path.join("data", "raw", "equity_prices_*.csv")

def main():
    # Check there is at least one CSV to load
    import glob
    files = glob.glob(GLOB)
    if not files:
        print(f"? No CSVs found at {GLOB}. Run the loader first: python .\\etl\\load_equities_daily.py")
        sys.exit(1)

    os.makedirs("warehouse", exist_ok=True)
    con = duckdb.connect(DB_PATH)

    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute("CREATE SCHEMA IF NOT EXISTS core;")

    # Rebuild raw table from all CSVs (simple and idempotent for now)
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.equity_prices AS
        SELECT 
            CAST(date AS DATE) AS date,
            CAST(open AS DOUBLE) AS open,
            CAST(high AS DOUBLE) AS high,
            CAST(low AS DOUBLE)  AS low,
            CAST(close AS DOUBLE) AS close,
            CAST(volume AS BIGINT) AS volume,
            UPPER(symbol) AS symbol
        FROM read_csv_auto('{GLOB}', header=True)
        ORDER BY symbol, date;
    """)

    # Simple core fact table with daily returns
    con.execute("""
        CREATE OR REPLACE TABLE core.fct_prices_daily AS
        SELECT
            symbol,
            date,
            close,
            volume,
            (close / LAG(close) OVER (PARTITION BY symbol ORDER BY date) - 1) AS ret_1d
        FROM raw.equity_prices
        ORDER BY symbol, date;
    """)

    # Quick sanity prints
    total_rows = con.execute("SELECT COUNT(*) FROM raw.equity_prices").fetchone()[0]
    symbols = con.execute("SELECT COUNT(DISTINCT symbol) FROM raw.equity_prices").fetchone()[0]
    preview = con.execute("SELECT * FROM core.fct_prices_daily ORDER BY symbol, date LIMIT 5").fetch_df()

    print(f"? Built DuckDB at {DB_PATH}")
    print(f"   raw.equity_prices rows: {total_rows} | symbols: {symbols}")
    print("   core.fct_prices_daily preview:")
    print(preview)

if __name__ == "__main__":
    main()
