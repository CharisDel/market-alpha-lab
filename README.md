
# Market Data Warehouse & Alpha Lab (starter)

A clean, beginner-friendly scaffold for the project we'll build together:
- Python ETL (data loaders) → SQL warehouse (later) → features → models → dashboard
- Simple and modular so you can learn Git, Python, and (later) SQL/dbt at a steady pace.

## What's included (MVP)
- Folder layout (see below)
- `.env.template` for your secrets/config (copy it to `.env` and fill values later)
- `requirements.txt` with light dependencies
- `etl/check_setup.py` to verify your environment is working

## Folder layout
```text
market-alpha-lab/
├─ README.md
├─ requirements.txt
├─ .gitignore
├─ .env.template
├─ app/
├─ etl/
│  └─ check_setup.py
├─ reports/
├─ sql/
├─ tests/
├─ dbt_project/     # placeholder (we'll set up dbt later)
└─ .vscode/         # optional editor settings
```

## Getting started (Windows PowerShell)
1. Open **PowerShell**.
2. Navigate to this folder (after unzipping or cloning):
   ```powershell
   cd path\to\market-alpha-lab
   ```
3. Create a virtual environment and activate it:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```
4. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```
5. Create your `.env` file from the template:
   ```powershell
   Copy-Item .env.template .env
   ```
   (We'll fill values later; for now defaults are ok.)

6. Verify your setup runs:
   ```powershell
   python .\etl\check_setup.py
   ```

## Next steps
- Initialize Git, make your first commit, and push to GitHub (instructions in our chat).
- After that, we'll add the first real ETL script and a small dataset.
