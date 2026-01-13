import streamlit as st
import duckdb
import os
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
import plotly.express as px
from utils import get_data_path

# ---------------------------------------------------------
# 1. PAGE CONFIGURATION
# ---------------------------------------------------------
st.set_page_config(
    page_title="InsiderZ", 
    page_icon="🐋", 
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Cpt. Ahab's Dashboard 🐋")
st.subheader("Tracking Insider Trading Signals with Data Lake & DuckDB")

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
            hovertemplate=(
                "<b>%{text}</b><br>" +
                "Date: %{x}<br>" +
                "Buy@ $%{y:.2f}<br>" +
                "<extra></extra>" # Removes the secondary box
            ),
            text=buys['owner_name'] + "<br>Val: $" + buys['total_value'].apply(lambda x: f"{x:,.0f}")
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
            hovertemplate=(
                "<b>%{text}</b><br>" +
                "Date: %{x}<br>" +
                "Sld@ $%{y:.2f}<br>" +
                "<extra></extra>" # Removes the secondary box
            ),
            text=sells['owner_name'] + " ($" + sells['total_value'].apply(lambda x: f"{x:,.0f}") + ")",
        ))

    fig.update_layout(
        title=f"{ticker}: Insider Entries & Exits vs Stock Price",
        xaxis_title="Date",
        yaxis_title="Price ($)",
        template="plotly_white",
        height=500,
        hovermode="x unified"
    )
    
    st.plotly_chart(fig, width='stretch')
    
# ---------------------------------------------------------
# 2. DATA CONNECTION (DuckDB + Cloud/Local Logic)
# ---------------------------------------------------------
@st.cache_resource
def get_database_connection():
    """
    Establishes a DuckDB connection and configures GCS authentication
    using the modern DuckDB 1.0 Secrets Manager.
    """
    try:
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        
        # Check environment for HMAC keys
        use_cloud = os.getenv("USE_CLOUD", "False") == "True"
        access_key = os.getenv("GCP_HMAC_ACCESS_KEY")
        secret_key = os.getenv("GCP_HMAC_SECRET")
        
        if use_cloud and access_key and secret_key:
            # Modern DuckDB 1.0+ Auth using HMAC Keys
            con.execute(f"""
                CREATE SECRET secret_gcs (
                    TYPE GCS,
                    KEY_ID '{access_key}',
                    SECRET '{secret_key}'
                );
            """)
        elif use_cloud:
            # Fallback: Try to rely on system-environment (Cloud Run often injects this automatically)
            # or the 'gcp_key.json' path if strictly necessary, but HMAC is preferred.
            # If you are local without HMAC keys, this part might fail on DuckDB 1.0+.
            # For local dev, ensure HMAC keys are in .env!
            pass

        return con
    except Exception as e:
        st.error(f"Failed to initialize database: {e}")
        st.stop()

con = get_database_connection()

# ---------------------------------------------------------
# 3. LOAD DATA VIEWS
# ---------------------------------------------------------
signals_buy_path = get_data_path("processed/gold_signals_buy_*.parquet")
signals_sell_path = get_data_path("processed/gold_signals_sell_*.parquet")
trades_path = get_data_path("processed/trades_*.parquet")

data_loaded = False

try:
    con.execute(f"CREATE OR REPLACE VIEW trades AS SELECT * FROM '{trades_path}'")
    
    # Load BUY Signals
    try:
        con.execute(f"CREATE OR REPLACE VIEW signals_buy AS SELECT * FROM '{signals_buy_path}'")
        has_buys = True
    except:
        has_buys = False

    # Load SELL Signals
    try:
        con.execute(f"CREATE OR REPLACE VIEW signals_sell AS SELECT * FROM '{signals_sell_path}'")
        has_sells = True
    except:
        has_sells = False
        
    data_loaded = True
except Exception as e:
    st.warning("No data found. Please run the Dagster pipeline first.")
    st.stop()

# ---------------------------------------------------------
# 4. DASHBOARD LAYOUT (TABS)
# ---------------------------------------------------------

tab_buys, tab_sells, tab_explorer = st.tabs(["📈 Good Buys", "📉 Good Sells", "🔍 Data Explorer"])

#  TAB 1: BUY SIGNALS 
with tab_buys:
    if not has_buys:
        st.info("No Buy Signals found.")
    else:
        st.header("📈 Top Buy Signals (Aggregated)")

        query_buy = """
            SELECT 
                filing_date, ticker, company_name, owner_name, owner_title, 
                MAX(conviction_score) as conviction_score,
                MAX(cluster_count) as cluster_size,
                SUM(total_value) as total_value_aggregated,
                SUM(shares) as total_shares,
                COUNT(*) as num_transactions,
                SUM(total_value) / NULLIF(SUM(shares), 0) as avg_price
            FROM signals_buy 
            GROUP BY filing_date, ticker, company_name, owner_name, owner_title
            HAVING total_value_aggregated > 0
            ORDER BY filing_date DESC, conviction_score DESC 
            LIMIT 100
        """

        df_buy = con.execute(query_buy).fetch_df()
        if not df_buy.empty:
            # Context function
            def generate_reason(row):
                reasons = []
                if "CEO" in str(row['owner_title']).upper() or "CFO" in str(row['owner_title']).upper(): reasons.append("👑 C-Suite")
                if row['total_value_aggregated'] > 500_000: reasons.append("💰 Whale")
                if row['cluster_size'] > 1: reasons.append(f"🤝 Cluster ({row['cluster_size']} buyers)")
                return ", ".join(reasons)
            
            df_buy['Signal Context'] = df_buy.apply(generate_reason, axis=1)
            
            st.dataframe(
                df_buy[["filing_date", "ticker", "owner_name", "owner_title", "total_value_aggregated", "conviction_score", "Signal Context"]].style
                .format({"total_value_aggregated": "${:,.0f}"})
                .background_gradient(subset=["conviction_score"], cmap="Greens"),
                width='stretch'
            )

# TAB 2: SELL SIGNALS 
with tab_sells:
    if not has_sells:
        st.info("No Sell Signals found.")
    else:
        st.header("📉 Top Sell Signals (Short Candidates)")
        st.markdown("""
        **Scoring Logic:**
        *   **+3 Points:** C-Suite Executive Dumping Stock
        *   **+3 Points:** Whale Dump (>$1M)
        *   **+2 Points:** Cluster Sell (Multiple insiders selling same day)
        """)
        
        query_sell = """
            SELECT 
                filing_date, ticker, company_name, owner_name, owner_title, 
                MAX(conviction_score) as conviction_score,
                MAX(cluster_count) as cluster_size,
                SUM(total_value) as total_value_aggregated,
                SUM(shares) as total_shares,
                COUNT(*) as num_transactions,
                SUM(total_value) / NULLIF(SUM(shares), 0) as avg_price
            FROM signals_sell
            GROUP BY filing_date, ticker, company_name, owner_name, owner_title
            HAVING total_value_aggregated > 0
            ORDER BY filing_date DESC, conviction_score DESC 
            LIMIT 100
        """
        
        df_sell = con.execute(query_sell).fetch_df()
        
        if not df_sell.empty:
            def generate_sell_reason(row):
                reasons = []
                if "CEO" in str(row['owner_title']).upper() or "CFO" in str(row['owner_title']).upper(): reasons.append("👑 C-Suite")
                if row['total_value_aggregated'] > 1_000_000: reasons.append("📉 Whale Dump")
                if row['cluster_size'] > 1: reasons.append(f"⚠️ Cluster ({row['cluster_size']} sellers)")
                return ", ".join(reasons)
            
            df_sell['Signal Context'] = df_sell.apply(generate_sell_reason, axis=1)
            
            # Display using RED gradient
            st.dataframe(
                df_sell[["filing_date", "ticker", "owner_name", "owner_title", "total_value_aggregated", "conviction_score", "Signal Context"]].style
                .format({"total_value_aggregated": "${:,.0f}"})
                .background_gradient(subset=["conviction_score"], cmap="Reds"),
                width='stretch'
            )

# --- TAB 3: RAW EXPLORER (The "Deep Dive" View) ---
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
        st.subheader("Net Insider Volume (Buys vs Sells)")
        
        # 1. Prepare Data for Plotting
        # We want Sells to appear as negative numbers so they point DOWN on the chart
        chart_df = df.copy()
        chart_df['plot_value'] = chart_df.apply(
            lambda x: x['total_value'] if x['transaction_code'] == 'P' else -x['total_value'], 
            axis=1
        )
        
        # 2. Group by Date and Transaction Code to aggregate daily volume
        # This merges the "Cluster" into a single bar per day/type
        daily_chart = chart_df.groupby(["transaction_date", "transaction_code"])['plot_value'].sum().reset_index()
        
        # 3. Create the Chart
        fig = px.bar(
            daily_chart, 
            x="transaction_date", 
            y="plot_value", 
            color="transaction_code",
            title="Daily Insider Sentiment (Green = Buying, Red = Selling)",
            # Custom Colors: Green for P, Red for S
            color_discrete_map={"P": "#2ecc71", "S": "#e74c3c"},
            # Custom Hover Data (Clean up the negative numbers in tooltip)
            hover_data={"plot_value": ":$,.0f"} 
        )
        
        # 4. Polish the Layout
        fig.update_layout(
            yaxis_title="Transaction Value ($)",
            xaxis_title="Date",
            legend_title="Type",
            # Add a zero line to separate buys/sells clearly
            shapes=[dict(type="line", x0=0, x1=1, y0=0, y1=0, xref="paper", line_width=1, line_color="black")]
        )
        
        # 5. Fix Tooltip format (Remove the negative sign for Sells visually)
        fig.update_traces(
            hovertemplate="<b>Date:</b> %{x}<br><b>Net Value:</b> $%{y:,.0f}<extra></extra>"
        )

        st.plotly_chart(fig, width='stretch')

        # Table
        def highlight_buy_sell(row):
            color = "#00ff3c73" if row.transaction_code == 'P' else "#ff505071"
            return [f'background-color: {color}'] * len(row)

        st.subheader("Transaction Details")
        
        st.dataframe(
            df.style.format({"total_value": "${:,.0f}", "price_per_share": "${:.2f}"})
            .apply(highlight_buy_sell, axis=1),
            width='stretch'
        )

        