
# 🐋 InsiderFlow: Cpt. Ahab's Data Lakehouse

**InsiderFlow** (aka *InsiderZ*) is an end-to-end data engineering pipeline that tracks Corporate Insider Trading (SEC Form 4) to identify "Unusual Whale Activity." 

It processes messy SGML/XML filings into a structured Data Lakehouse, scoring trades to differentiate between standard executive compensation and **High Conviction Signals** (both Long and Short).

## 🏗 Architecture
This project implements a **Hybrid Cloud Lakehouse** architecture:

*   **Ingestion:** Asynchronous Python scraper (`httpx`, `tenacity`) extracts daily filings from SEC EDGAR.
*   **Orchestration:** **Dagster** manages the asset dependency graph, partitions (backfilling), and data lineage.
*   **Storage:** **Google Cloud Storage** (or Local Filesystem) using a Delta/Parquet structure.
*   **Processing:** **Polars** (Rust-based DataFrame) & **Pydantic** for strict schema validation and scoring logic.
*   **Analytics:** **DuckDB** performs OLAP queries directly on the raw Parquet files.
*   **Visualization:** **Streamlit** dashboard with **Plotly** charts for interactive financial analysis.

## 🛠 Tech Stack
*   **Language:** Python 3.12+
*   **Package Manager:** [uv](https://github.com/astral-sh/uv) (Blazing fast pip replacement)

---

## 🚀 Local Setup Guide

Follow these steps to run the pipeline and dashboard on your own machine.

### 1. Prerequisites
*   Install [Git](https://git-scm.com/downloads)
*   Install [uv](https://github.com/astral-sh/uv) (The Python package manager)

### 2. Installation
Clone the repository and sync dependencies:

```bash
git clone https://github.com/ozan97/insiderZ.git
cd insider-flow

# This installs Python and all libraries into a local .venv
uv sync
```

### 3. Configuration (Environment Variables)
Create a `.env` file in the root directory to configure the app.

**Option A: Local Mode (Simplest)**
Runs everything on your hard drive. No Cloud account needed.
```ini
# .env
USE_CLOUD=False
DAGSTER_HOME=.dagster
```

**Option B: Hybrid Cloud Mode (Optional)**
If you want to read/write to Google Cloud Storage:
1.  Place your Service Account Key as `gcp_key.json` in the root.
2.  Update `.env`:
```ini
# .env
USE_CLOUD=True
DAGSTER_HOME=.dagster
GCS_BUCKET_NAME=your-bucket-name
```

### 4. Run the Data Pipeline (Dagster)
Start the Orchestration UI:

```bash
uv run dagster dev -m src.insider_flow
```
*   Open [http://localhost:3000](http://localhost:3000).
*   Click **"Overview"** -> **"Backfills"** -> **"Launch Backfill"**.
*   Select a date range (e.g., last 7 days) to download and process SEC data.

### 5. Run the Analyst Dashboard (Streamlit)
Once you have materialized data, launch the frontend:

```bash
uv run streamlit run src/insider_flow/dashboard.py
```
*   Open the URL printed in the terminal (usually http://localhost:8501).
*   Explore "High Conviction Signals" (Buys/Sells) or search for specific tickers.

---

## What is a "Signal"?
The dashboard doesn't just list trades; it aggregates split orders and assigns a **Conviction Score (0-10)** based on:

1.  **The Boss (+3):** Is the buyer C-Suite (CEO/CFO)?
2.  **The Whale (+3):** Is the total trade value >$500k (Buys) or >$1M (Sells)?
3.  **The Cluster (+2):** Did multiple insiders act on the same stock on the same day?

The Dashboard separates these into:
*   **Buy Signals:** Insiders betting *on* the company.
*   **Sell Signals:** Insiders running *for the exits*.

---

## To-Dos


- [ ] **Whale Reputation (Backtesting):** 
    - Implement a pipeline to ingest historical stock data (`yfinance`).
    - Calculate the 6-month "Win Rate" for every insider (e.g., "CEO has a 90% accuracy").
    - Display "Smart Money" badges next to insiders with proven track records.
    
- [ ] **Market Context (Confluence):** 
    - Enrich signals with live market metrics.
    - **Dip Score:** Is the stock near its 52-week low? (Value Signal).
    - **Valuation:** Is P/E < 15? (Cheap) or > 50? (Growth).
    - **RSI:** Is the stock oversold? (Panic buying signal).

- [ ] **Active Alerting:** 
    - Create a Discord/Slack webhook asset.
    - Trigger push notifications when a "Perfect Storm" trade occurs (Score ≥ 8).
```