
import os
import sys
import pandas as pd
import numpy as np
import yfinance as yf
from dotenv import load_dotenv

def main():
    load_dotenv()
    print("✅ Python OK:", sys.version.split()[0])
    print("✅ pandas:", pd.__version__)
    print("✅ numpy:", np.__version__)
    print("✅ yfinance:", yf.__version__)

    db_url = os.getenv("DATABASE_URL", "(not set)")
    fred_key = os.getenv("FRED_API_KEY", "(not set)")
    print("ENV DATABASE_URL:", db_url)
    print("ENV FRED_API_KEY:", "(set)" if fred_key else "(not set)")

    # Tiny smoke test: fetch 5 days of SPY prices (no DB yet)
    df = yf.download("SPY", period="5d", interval="1d", progress=False)
    if isinstance(df, pd.DataFrame) and not df.empty:
        print("✅ yfinance fetch OK. Rows:", len(df))
    else:
        print("⚠️ yfinance fetch returned no rows. Check internet connection.")

    print("All good! You're ready for Git init and first commit.")

if __name__ == "__main__":
    main()
