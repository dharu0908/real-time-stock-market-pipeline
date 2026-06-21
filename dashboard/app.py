import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2

st.set_page_config(
    page_title="Stock Market Dashboard",
    layout="wide"
)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "stockdb",
    "user": "stockuser",
    "password": "stockpass",
}

@st.cache_data(ttl=10)
def load_data():
    conn = psycopg2.connect(**DB_CONFIG)

    query = """
    SELECT *
    FROM gold_ohlcv_1min
    ORDER BY window_start DESC
    LIMIT 1000
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df


df = load_data()

if df.empty:
    st.warning("No data available.")
    st.stop()

df["window_start"] = pd.to_datetime(df["window_start"])
df = df.sort_values("window_start")

st.title("Real-Time Stock Analytics Platform")
st.caption(
    "Streaming data pipeline built with Kafka, Spark Structured Streaming, PostgreSQL, and Streamlit"
)

# Sidebar filter
with st.sidebar:
    st.header("Filters")

    selected_ticker = st.selectbox(
        "Select Stock",
        sorted(df["ticker"].unique())
    )

    records = st.slider(
        "Recent Records",
        min_value=30,
        max_value=300,
        value=120,
        step=30
    )

# Overview KPIs
latest_market = (
    df.sort_values("window_start")
    .groupby("ticker")
    .tail(1)
)

active_tickers = latest_market["ticker"].nunique()
total_volume = int(latest_market["volume"].sum())
total_ticks = int(latest_market["tick_count"].sum())
latest_update = df["window_start"].max()

st.subheader("Overview")

o1, o2, o3, o4 = st.columns(4)

o1.metric("Active Tickers", active_tickers)
o2.metric("Total Volume", f"{total_volume:,}")
o3.metric("Total Ticks", f"{total_ticks:,}")
o4.metric("Latest Update", latest_update.strftime("%H:%M:%S"))

st.divider()

# Selected stock data
ticker_df = (
    df[df["ticker"] == selected_ticker]
    .sort_values("window_start")
    .tail(records)
)

latest = ticker_df.iloc[-1]

st.subheader(f"{selected_ticker} Details")

# Stock KPIs aligned above chart
k1, k2, k3, k4 = st.columns(4)

k1.metric("Close Price", f"${latest['close']:.2f}")
k2.metric("VWAP", f"${latest['vwap']:.2f}")
k3.metric("Volume", f"{int(latest['volume']):,}")
k4.metric("Avg Spread %", f"{latest['avg_spread_pct']:.4f}")

# Price chart
price_fig = px.line(
    ticker_df,
    x="window_start",
    y="close",
    title=f"{selected_ticker} Price Trend"
)

price_fig.update_layout(
    height=330,
    margin=dict(l=10, r=10, t=50, b=10),
    xaxis_title="Time",
    yaxis_title="Price"
)

st.plotly_chart(price_fig, use_container_width=True)

# Latest records
st.subheader(
    "Recent OHLCV Candles (Open, High, Low, Close, Volume)"
)

display_cols = [
    "window_start",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "tick_count",
    "vwap"
]

st.dataframe(
    ticker_df[display_cols]
    .sort_values("window_start", ascending=False)
    .head(10),
    use_container_width=True,
    height=300
)

st.caption("OHLCV = Open, High, Low, Close, Volume")