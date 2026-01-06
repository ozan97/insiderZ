import streamlit as st
import duckdb
import os
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
import plotly.express as px
from utils import get_data_path, get_storage_options

# ---------------------------------------------------------
# 1. PAGE CONFIGURATION
# ---------------------------------------------------------
st.set_page_config(
    page_title="InsiderFlow", 
    page_icon="🐋", 
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🐋 Cpt. Ahab's Dashboard: Insider Trading Tracker")
st.markdown("### *Harnessing Data Engineering to find the Whales*")

# Helper functions
@st.cache_data(ttl=3600) # Cache data for 1 hour so it's fast
def get_stock_history(ticker):
    """
    Fetches 1 year of daily stock history from Yahoo Finance.
    """
    try:
        # Fetch 1 year of data
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1y")
        return hist.reset_index()
    except Exception:
        return pd.DataFrame()

def render_chart(ticker, trades_df):
    """
    Draws a line chart of stock price with insider trades overlaid as markers.
    """
    # 1. Get Stock History
    stock_df = get_stock_history(ticker)
    if stock_df.empty:
        st.warning(f"Could not fetch stock history for {ticker}")
        return

    # 2. Create the Base Line Chart (Stock Price)
    fig = go.Figure()

    # Add Stock Price Line
    fig.add_trace(go.Scatter(
        x=stock_df['Date'], 
        y=stock_df['Close'],
        mode='lines',
        name=f'{ticker} Price',
        line=dict(color='gray', width=1)
    ))

    # 3. Overlay Insider Buys (Green Triangles)
    buys = trades_df[trades_df['transaction_code'] == 'P']
    if not buys.empty:
        fig.add_trace(go.Scatter(
            x=buys['transaction_date'],
            y=buys['price_per_share'], # Use the price they actually paid
            mode='markers',
            name='Insider Buy',
            marker=dict(symbol='triangle-up', size=12, color='#00CC96', line=dict(width=1, color='black')),
            text=buys['owner_name'] + " ($" + buys['total_value'].apply(lambda x: f"{x:,.0f}") + ")",
            hoverinfo='text+x+y'
        ))

    # 4. Overlay Insider Sells (Red Triangles Down)
    sells = trades_df[trades_df['transaction_code'] == 'S']
    if not sells.empty:
        fig.add_trace(go.Scatter(
            x=sells['transaction_date'],
            y=sells['price_per_share'],
            mode='markers',
            name='Insider Sell',
            marker=dict(symbol='triangle-down', size=12, color='#EF553B', line=dict(width=1, color='black')),
            text=sells['owner_name'] + " ($" + sells['total_value'].apply(lambda x: f"{x:,.0f}") + ")",
            hoverinfo='text+x+y'
        ))

    fig.update_layout(
        title=f"{ticker}: Insider Entries & Exits vs Stock Price",
        xaxis_title="Date",
        yaxis_title="Price ($)",
        template="plotly_white",
        height=500,
        hovermode="x unified"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
# ---------------------------------------------------------
# 2. DATA CONNECTION (DuckDB + Cloud/Local Logic)
# ---------------------------------------------------------
@st.cache_resource
def get_database_connection():
    """
    Establishes a DuckDB connection and configures it for GCS if needed.
    Cached resource to avoid reloading on every interaction.
    """
    try:
        con = duckdb.connect(database=":memory:")
        
        # Configure GCS if we are in Cloud Mode or have keys
        storage_opts = get_storage_options()
        use_cloud = os.getenv("USE_CLOUD", "False") == "True"
        
        if "google_application_credentials" in storage_opts:
            # Local Mode with Key File
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute(f"SET google_credentials = '{storage_opts['google_application_credentials']}';")
        elif use_cloud:
            # Cloud Run Mode (Auto Auth)
            con.execute("INSTALL httpfs; LOAD httpfs;")

        return con
    except Exception as e:
        st.error(f"Failed to initialize database: {e}")
        st.stop()

con = get_database_connection()

# ---------------------------------------------------------
# 3. LOAD DATA VIEWS
# ---------------------------------------------------------
# We create two views: 'signals' (The Gold Layer) and 'trades' (The Silver/Processed Layer)

signals_path = get_data_path("processed/gold_signals.parquet")
trades_path = get_data_path("processed/trades_*.parquet") # Glob pattern for daily files

data_loaded = False

try:
    # Load Raw Trades View
    con.execute(f"CREATE OR REPLACE VIEW trades AS SELECT * FROM '{trades_path}'")
    
    # Load Signals View (Handle case where pipeline hasn't run 'signals' asset yet)
    try:
        con.execute(f"CREATE OR REPLACE VIEW signals AS SELECT * FROM '{signals_path}'")
        has_signals = True
    except Exception:
        has_signals = False
        
    data_loaded = True
except Exception as e:
    st.warning("No data found. Please run the Dagster pipeline first.")
    st.stop()

# ---------------------------------------------------------
# 4. DASHBOARD LAYOUT (TABS)
# ---------------------------------------------------------
tab_signals, tab_explorer = st.tabs(["🔥 High Conviction Signals", "🔍 Raw Data Explorer"])

# --- TAB 1: SIGNALS (The "Smart" View) ---
with tab_signals:
    if not has_signals:
        st.info("No High Conviction Signals generated yet. Run the 'signals' asset in Dagster.")
    else:
        st.header("Top Buy Signals (Score ≥ 5)")
        st.markdown("""
        **Scoring Logic:**
        *   **+3 Points:** C-Suite Executive (CEO/CFO)
        *   **+3 Points:** Whale Buy (>$500k)
        *   **+2 Points:** Cluster Buy (Multiple insiders buying same day)
        """)
        
        # Query Signals
        sig_df = con.execute("""
            SELECT 
                filing_date, ticker, company_name, owner_name, owner_title, 
                total_value, daily_buyer_count, conviction_score
            FROM signals 
            ORDER BY filing_date DESC, conviction_score DESC 
            LIMIT 100
        """).fetch_df()
        
        if not sig_df.empty:
            # Stylized Dataframe
            st.dataframe(
                sig_df.style.format({"total_value": "${:,.0f}"})
                .background_gradient(subset=["conviction_score"], cmap="Greens"),
                use_container_width=True
            )
        else:
            st.write("No signals found recently.")

# --- TAB 2: RAW EXPLORER (The "Deep Dive" View) ---
with tab_explorer:
    st.header("Search the Data Lake")
    
    # Sidebar Filters (Only apply to this tab logic usually, but here global for simplicity)
    col1, col2 = st.columns(2)
    with col1:
        selected_ticker = st.text_input("Filter by Ticker (e.g. NVDA)", "").upper()
    with col2:
        min_val = st.number_input("Min Value ($)", value=10000, step=10000)

    # Build Query
    query = f"""
        SELECT filing_date, transaction_date, ticker, owner_name, owner_title, transaction_code, price_per_share, total_value
        FROM trades 
        WHERE total_value >= {min_val}
    """
    
    if selected_ticker:
        query += f" AND ticker = '{selected_ticker}'"
    
    query += " ORDER BY transaction_date DESC LIMIT 500"
    
    # Execute
    df = con.execute(query).fetch_df()
    
    # Metrics
    m1, m2 = st.columns(2)
    m1.metric("Visible Trades", f"{len(df):,}")
    if not df.empty:
        m2.metric("Avg Trade Size", f"${df['total_value'].mean():,.0f}")
    
    st.divider()

    # Visuals
    if selected_ticker and not df.empty:
        render_chart(selected_ticker, df)

        
    elif not df.empty:
        # Chart
        chart_df = df.groupby(["transaction_date", "transaction_code"])["total_value"].sum().reset_index()
        fig = px.bar(
            chart_df, x="transaction_date", y="total_value", color="transaction_code",
            title="Volume by Day (Buy vs Sell)",
            color_discrete_map={"P": "#00ff00", "S": "#ff0000"}
        )
        st.plotly_chart(fig, use_container_width=True)

        # Table
        def highlight_buy_sell(row):
            color = '#d4edda' if row.transaction_code == 'P' else '#f8d7da'
            return [f'background-color: {color}'] * len(row)

        st.subheader("Transaction Details")
        
        st.dataframe(
            df.style.format({"total_value": "${:,.0f}", "price_per_share": "${:.2f}"})
            .apply(highlight_buy_sell, axis=1),
            use_container_width=True
        )