import streamlit as st
import duckdb
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import yfinance as yf
from utils import get_data_path

st.set_page_config(page_title="InsiderFlow", page_icon="🐋", layout="wide")

# --- DATA LOADING (CACHED) ---
@st.cache_data(ttl=3600)
def load_data_from_gcs():
    """Connects to GCS, aggregates data, and returns a local DataFrame."""
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # Auth using HMAC keys (DuckDB 1.0 Secrets Manager)
    access_key = os.getenv("GCP_HMAC_ACCESS_KEY")
    secret_key = os.getenv("GCP_HMAC_SECRET")
    
    if access_key and secret_key:
        con.execute(f"""
            CREATE SECRET secret_gcs (
                TYPE GCS,
                KEY_ID '{access_key}',
                SECRET '{secret_key}'
            );
        """)

    # Use wildcards to read all partitions
    signals_path = get_data_path("processed/enriched_signals_*.parquet")
    
    # Load into memory as Pandas for fast Streamlit filtering
    return con.execute(f"SELECT * FROM '{signals_path}'").df()

@st.cache_data(ttl=3600)
def get_stock_history(ticker):
    """Fetch 1 year of price data."""
    try:
        return yf.Ticker(ticker).history(period="1y").reset_index()
    except Exception:
        return pd.DataFrame()

# --- UI LOGIC ---
st.title("🐋 Cpt. Ahab's Dashboard")

try:
    raw_df = load_data_from_gcs()
except Exception as e:
    st.error(f"Cloud Connection Failed: {e}")
    st.stop()

# SIDEBAR FILTERS
st.sidebar.header("Filter Whale Signals")
min_score = st.sidebar.slider("Minimum Ahab Score", 0, 20, 10)
ticker_search = st.sidebar.text_input("Ticker Search (e.g., NVDA)").upper()

# --- TAB 1: ELITE SIGNALS ---
tab_signals, tab_chart = st.tabs(["🔥 High Conviction", "📉 Stock Analysis"])

with tab_signals:
    # Local filtering (instant)
    filtered_df = raw_df[raw_df['ahab_score'] >= min_score]
    if ticker_search:
        filtered_df = filtered_df[filtered_df['ticker'] == ticker_search]

    st.subheader(f"Found {len(filtered_df)} Elite Signals")
    
    cols = ["filing_date", "ticker", "owner_name", "total_value", "ahab_score", "pe_ratio", "dip_from_52w_high"]
    st.dataframe(
        filtered_df[cols].style.format({
            "total_value": "${:,.0f}",
            "pe_ratio": "{:.1f}",
            "dip_from_52w_high": "{:.1%}"
        }).background_gradient(subset=["ahab_score"], cmap="YlOrRd"),
        width='stretch'
    )

with tab_chart:
    if not ticker_search:
        st.info("Enter a ticker in the sidebar to see price correlation.")
    else:
        hist = get_stock_history(ticker_search)
        ticker_trades = raw_df[raw_df['ticker'] == ticker_search]
        
        if hist.empty:
            st.warning("No price data found.")
        else:
            fig = go.Figure()
            # Price Line
            fig.add_trace(go.Scatter(x=hist['Date'], y=hist['Close'], name='Price', line=dict(color='silver')))
            
            # Overlay Buys
            buys = ticker_trades[ticker_trades['transaction_code'] == 'P']
            fig.add_trace(go.Scatter(
                x=buys['transaction_date'], y=buys['price_per_share'],
                mode='markers', name='Insider Buy',
                marker=dict(symbol='triangle-up', size=12, color='green', line=dict(width=1, color='black'))
            ))
            
            fig.update_layout(height=600, template="plotly_white", title=f"{ticker_search} Buy Correlation")
            st.plotly_chart(fig, width='stretch')