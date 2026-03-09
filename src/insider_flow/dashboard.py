import streamlit as st
import duckdb
import os
import pandas as pd
import plotly.graph_objects as go
import yfinance as yf
from insider_flow.utils import get_data_path

st.set_page_config(page_title="InsiderFlow", page_icon="🐋", layout="wide")

# ---------------------------------------------------------------------------
# DATA LOADING
# ---------------------------------------------------------------------------

def _duckdb_con():
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    access_key = os.getenv("GCP_HMAC_ACCESS_KEY")
    secret_key = os.getenv("GCP_HMAC_SECRET")
    if access_key and secret_key:
        safe_ak = access_key.replace("'", "''")
        safe_sk = secret_key.replace("'", "''")
        con.execute(f"""
            CREATE SECRET secret_gcs (
                TYPE GCS, KEY_ID '{safe_ak}', SECRET '{safe_sk}'
            );
        """)
    return con


def _safe_read(con, pattern: str) -> pd.DataFrame:
    path = get_data_path(pattern)
    try:
        return con.execute(f"SELECT * FROM '{path}'").df()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_trades():
    con = _duckdb_con()
    return _safe_read(con, "processed/scored_trades_*.parquet")


@st.cache_data(ttl=3600)
def load_profiles():
    con = _duckdb_con()
    return _safe_read(con, "profiles/insider_profiles_*.parquet")


@st.cache_data(ttl=3600)
def load_track_records():
    con = _duckdb_con()
    return _safe_read(con, "profiles/track_records_*.parquet")


@st.cache_data(ttl=3600)
def get_stock_history(ticker: str):
    try:
        return yf.Ticker(ticker).history(period="1y").reset_index()
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------------------------

st.title("🐋 Cpt. Ahab's Dashboard")

trades_df = load_trades()
profiles_df = load_profiles()
records_df = load_track_records()

if trades_df.empty:
    st.warning("No trade data found yet. Run the Dagster pipeline first.")
    st.stop()

# ---------------------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------------------

st.sidebar.header("Filters")
min_score = st.sidebar.slider("Min Conviction Score", 0, 15, 0)
tx_type = st.sidebar.selectbox("Trade Type", ["All", "Buys (P)", "Sells (S)"])
ticker_search = st.sidebar.text_input("Ticker (e.g. NVDA)").upper().strip()
insider_search = st.sidebar.text_input("Insider Name (partial match)").upper().strip()

# ---------------------------------------------------------------------------
# FILTERING
# ---------------------------------------------------------------------------

filtered = trades_df.copy()
if min_score > 0:
    filtered = filtered[filtered["conviction_score"] >= min_score]
if tx_type == "Buys (P)":
    filtered = filtered[filtered["transaction_code"] == "P"]
elif tx_type == "Sells (S)":
    filtered = filtered[filtered["transaction_code"] == "S"]
if ticker_search:
    filtered = filtered[filtered["ticker"] == ticker_search]
if insider_search:
    filtered = filtered[filtered["owner_name"].str.upper().str.contains(insider_search, na=False)]

# ---------------------------------------------------------------------------
# TABS
# ---------------------------------------------------------------------------

tab_trades, tab_insider, tab_chart = st.tabs([
    "📋 All Trades", "🔍 Insider Lookup", "📉 Stock Analysis"
])

# --- TAB 1: All scored trades ---
with tab_trades:
    st.subheader(f"{len(filtered)} Trades")
    display_cols = [
        c for c in [
            "filing_date", "ticker", "transaction_code", "owner_name",
            "total_value", "shares", "price_per_share", "conviction_score",
            "score_role", "score_cluster", "cluster_size",
        ] if c in filtered.columns
    ]
    st.dataframe(
        filtered[display_cols].sort_values("conviction_score", ascending=False),
        use_container_width=True,
    )

# --- TAB 2: Insider Lookup ---
with tab_insider:
    if insider_search:
        # Match from profiles
        if not profiles_df.empty and "owner_name" in profiles_df.columns:
            matched = profiles_df[profiles_df["owner_name"].str.upper().str.contains(insider_search, na=False)]
        else:
            matched = pd.DataFrame()

        if matched.empty:
            st.info("No insider profile found. Showing trades matching name.")
        else:
            for _, profile in matched.iterrows():
                cik = profile.get("owner_cik", "")
                st.subheader(f"{profile.get('owner_name', 'Unknown')}")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Buys", int(profile.get("total_buys", 0)))
                col2.metric("Total Sells", int(profile.get("total_sells", 0)))
                col3.metric("Buy Value", f"${profile.get('total_buy_value', 0):,.0f}")
                col4.metric("Sell Value", f"${profile.get('total_sell_value', 0):,.0f}")

                # Show track record if available
                if not records_df.empty and "owner_cik" in records_df.columns:
                    rec = records_df[records_df["owner_cik"] == cik]
                    if not rec.empty:
                        r = rec.iloc[0]
                        st.markdown("**Track Record**")
                        rc1, rc2, rc3, rc4 = st.columns(4)
                        rc1.metric("Trades Evaluated", int(r.get("total_trades_evaluated", 0)))
                        wr30 = r.get("win_rate_30d")
                        rc2.metric("Win Rate (30d)", f"{wr30:.0%}" if pd.notna(wr30) else "N/A")
                        ar30 = r.get("avg_return_30d")
                        rc3.metric("Avg Return (30d)", f"{ar30:.1%}" if pd.notna(ar30) else "N/A")
                        wr90 = r.get("win_rate_90d")
                        rc4.metric("Win Rate (90d)", f"{wr90:.0%}" if pd.notna(wr90) else "N/A")

                st.markdown("---")

        # Always show trade history for this person
        st.subheader("Trade History")
        person_trades = filtered if insider_search else pd.DataFrame()
        if not person_trades.empty:
            hist_cols = [c for c in [
                "filing_date", "ticker", "transaction_code", "shares",
                "price_per_share", "total_value", "conviction_score",
            ] if c in person_trades.columns]
            st.dataframe(
                person_trades[hist_cols].sort_values("filing_date", ascending=False),
                use_container_width=True,
            )
        else:
            st.info("No trades found for this insider.")
    else:
        st.info("Enter an insider name in the sidebar to look them up.")

        # Show top insiders by track record
        if not records_df.empty:
            st.subheader("Top Insiders by Win Rate (30d)")
            top_cols = [c for c in [
                "owner_name", "total_trades_evaluated", "win_rate_30d",
                "avg_return_30d", "win_rate_90d", "avg_return_90d",
            ] if c in records_df.columns]
            top = records_df[records_df.get("total_trades_evaluated", pd.Series([0])) >= 3]
            if not top.empty:
                st.dataframe(
                    top[top_cols].sort_values("win_rate_30d", ascending=False).head(25),
                    use_container_width=True,
                )

# --- TAB 3: Stock Analysis (price chart + insider overlays) ---
with tab_chart:
    if not ticker_search:
        st.info("Enter a ticker in the sidebar to see price analysis.")
    else:
        hist = get_stock_history(ticker_search)
        ticker_trades = trades_df[trades_df["ticker"] == ticker_search]

        if hist.empty:
            st.warning("No price data found.")
        else:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=hist["Date"], y=hist["Close"], name="Price",
                line=dict(color="silver"),
            ))

            # Overlay buys
            buys = ticker_trades[ticker_trades["transaction_code"] == "P"]
            if not buys.empty:
                fig.add_trace(go.Scatter(
                    x=buys["transaction_date"], y=buys["price_per_share"],
                    mode="markers", name="Insider Buy",
                    marker=dict(symbol="triangle-up", size=12, color="green",
                                line=dict(width=1, color="black")),
                    text=buys["owner_name"],
                ))

            # Overlay sells
            sells = ticker_trades[ticker_trades["transaction_code"] == "S"]
            if not sells.empty:
                fig.add_trace(go.Scatter(
                    x=sells["transaction_date"], y=sells["price_per_share"],
                    mode="markers", name="Insider Sell",
                    marker=dict(symbol="triangle-down", size=12, color="red",
                                line=dict(width=1, color="black")),
                    text=sells["owner_name"],
                ))

            fig.update_layout(
                height=600, template="plotly_white",
                title=f"{ticker_search} — Insider Activity Overlay",
            )
            st.plotly_chart(fig, use_container_width=True)