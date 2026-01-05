import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# Page Config
st.set_page_config(page_title="InsiderFlow", page_icon="📈", layout="wide")

st.title("🐋 InsiderFlow: Corporate Insider Trading Tracker")

# 1. Connect to the Data Lake (DuckDB)
# We use read_parquet with a glob pattern to read ALL processed files
try:
    con = duckdb.connect(database=":memory:")
    # Create a view over all parquet files
    con.execute("CREATE OR REPLACE VIEW trades AS SELECT * FROM 'data/processed/*.parquet'")
    
    # Check if we have data
    count = con.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    if count == 0:
        st.warning("No data found in data/processed/. Run the Dagster pipeline first!")
        st.stop()
        
except Exception as e:
    st.error(f"Could not connect to data lake: {e}")
    st.stop()

# 2. Sidebar Filters
st.sidebar.header("Filters")
selected_ticker = st.sidebar.text_input("Ticker Symbol (e.g. AAPL)", "").upper()
min_value = st.sidebar.number_input("Min Transaction Value ($)", min_value=0, value=10000, step=10000)

# 3. Main Query
base_query = f"""
    SELECT 
        filing_date,
        transaction_date,
        ticker, 
        company_name, 
        owner_name, 
        owner_title, 
        transaction_code, 
        shares, 
        price_per_share, 
        total_value
    FROM trades
    WHERE total_value >= {min_value}
"""

if selected_ticker:
    base_query += f" AND ticker = '{selected_ticker}'"

# Order by latest
base_query += " ORDER BY transaction_date DESC LIMIT 1000"

# Execute
df = con.execute(base_query).fetch_df()

# 4. Metrics Row
col1, col2, col3 = st.columns(3)
col1.metric("Total Transactions Loaded", f"{count:,}")
col2.metric("Filtered Trades", f"{len(df):,}")
if not df.empty:
    avg_price = df["price_per_share"].mean()
    col3.metric("Avg Price (Filtered)", f"${avg_price:,.2f}")

# 5. Visuals
if not df.empty:
    # Chart: Buy/Sell Volume over time
    st.subheader("Transaction Volume Over Time")
    chart_df = df.groupby(["transaction_date", "transaction_code"])["total_value"].sum().reset_index()
    fig = px.bar(chart_df, x="transaction_date", y="total_value", color="transaction_code", 
                 title="Daily Insider Volume (Buy vs Sell)",
                 color_discrete_map={"P": "green", "S": "red"})
    st.plotly_chart(fig, width='stretch')

    # Data Grid
    st.subheader("Recent Filings")
    
    # Highlight buys in green, sells in red
    def color_coding(row):
        return ['background-color: #d4edda' if row.transaction_code == 'P' else 'background-color: #f8d7da'] * len(row)

    st.dataframe(
        df.style.format({"total_value": "${:,.0f}", "price_per_share": "${:.2f}"}),
        width='stretch'
    )
else:
    st.info("No trades found matching your filters.")