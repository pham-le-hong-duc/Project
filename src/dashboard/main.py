"""
Streamlit Dashboard for Index Price Klines
Display candlestick charts using lightweight-charts
"""
import streamlit as st
from lightweight_charts.widgets import StreamlitChart
from data_loader import DataLoader
from datetime import datetime, timedelta
import pandas as pd

# Page configuration
st.set_page_config(
    page_title="Index Price Klines Dashboard",
    page_icon="📈",
    layout="wide"
)

# Initialize data loader
@st.cache_resource
def get_data_loader():
    return DataLoader()

loader = get_data_loader()

# Sidebar configuration
with st.sidebar.expander("📊 Kline", expanded=True):
    interval = st.selectbox("Interval", options=['5m', '15m', '1h', '4h', '1d'], index=2)
    data_range_option = st.radio("Data Range", options=['Latest N candles', 'Date Range'], index=0)
    
    if data_range_option == 'Latest N candles':
        stats = loader.get_statistics(interval=interval)
        max_available = stats.get('total_records', 1000)
        n_candles = st.slider("Number of candles", 50, max_available, min(200, max_available), 50)
    else:
        min_date, max_date = loader.get_available_date_range(interval=interval)
        if min_date and max_date:
            st.info(f"📅 {min_date.date()} to {max_date.date()}")
            default_start = max(min_date, max_date - timedelta(days=7))
            start_date = st.date_input("Start Date", default_start.date(), min_value=min_date.date(), max_value=max_date.date())
            end_date = st.date_input("End Date", max_date.date(), min_value=min_date.date(), max_value=max_date.date())
        else:
            st.error("No data"); st.stop()

# Load data
if data_range_option == 'Latest N candles':
    df = loader.get_latest_records(interval=interval, n=n_candles)
else:
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())
    df = loader.load_ohlc(interval=interval, start_date=start_datetime, end_date=end_datetime)

if df is None or len(df) == 0:
    st.warning("No data"); st.stop()

# Prepare data
chart_data = df[['time', 'open', 'high', 'low', 'close']].copy()
chart_data['time'] = chart_data['time'].dt.strftime('%Y-%m-%d %H:%M:%S')

# --- CSS STYLING ---
st.markdown("""
<style>
    /* Title styling - đẩy lên cao hơn */
    h1 {
        margin-top: -60px !important;
        padding-top: 0 !important;
    }
    
    /* 1. Tùy chỉnh Iframe của Chart (Bo tròn 2 góc trên) */
    iframe {
        border-top-left-radius: 15px !important;
        border-top-right-radius: 15px !important;
        border-bottom-left-radius: 0px !important;
        border-bottom-right-radius: 0px !important;
        margin-bottom: -6px !important;
        display: block;
        overflow: hidden !important;
        clip-path: inset(0 0 0 0 round 15px 15px 0 0) !important;
    }
    
    /* Force border radius on container */
    .stIFrame > div {
        border-top-left-radius: 15px !important;
        border-top-right-radius: 15px !important;
        overflow: hidden !important;
    }

    /* 2. Tùy chỉnh thanh Metrics bên dưới (Bo tròn 2 góc dưới) */
    .metrics-container {
        background-color: #1E1E1E;
        color: #D1D4DC;
        padding: 15px 20px; /* Padding vừa phải cho khung nhỏ */
        border-bottom-left-radius: 15px;
        border-bottom-right-radius: 15px;
        font-family: 'Arial', sans-serif;
        display: flex;
        flex-wrap: wrap; /* Cho phép xuống dòng nếu quá chật */
        justify-content: space-between;
        align-items: center;
        gap: 10px; /* Khoảng cách giữa các chỉ số */
    }

    /* Style cho từng chỉ số */
    .metric-item {
        text-align: left;
        min-width: 60px; /* Đảm bảo không bị co quá nhỏ */
    }
    
    .metric-label {
        font-size: 11px;
        color: #787B86;
        margin-bottom: 2px;
    }
    
    .metric-value {
        font-size: 16px; /* Giảm font size một chút cho vừa khung 1/3 */
        font-weight: 600;
        color: #E0E3EB;
    }

    .metric-change-pos { color: #26a69a; font-size: 12px; font-weight: normal; }
    .metric-change-neg { color: #ef5350; font-size: 12px; font-weight: normal; }

</style>
""", unsafe_allow_html=True)

# Title
st.title("📈 OKX BTC-USDT Dashboard")

# --- MAIN LAYOUT (1.05x Width) ---
# Chiều rộng: Cột 1 (1.05 phần), Cột 2 (0.95 phần - để trống)
col_main, col_space = st.columns([0.95, 0.95])

with col_main:
    st.subheader(f"Index Price ({interval})")

    # Tính toán các chỉ số
    close_price = df['close'].iloc[-1]
    open_price = df['open'].iloc[0]
    high_price = df['high'].max()
    low_price = df['low'].min()
    change_abs = close_price - open_price
    change_pct = (change_abs / open_price) * 100

    # Tạo Chart
    # Lưu ý: width="100%" để chart tự co giãn theo chiều rộng của cột col_main
    chart = StreamlitChart(height=270, width="100%") 
    chart.set(chart_data)

    chart.layout(
        background_color='#1E1E1E',
        text_color='#D1D4DC',
        font_size=12,
        font_family='Arial'
    )

    chart.candle_style(
        up_color='#26a69a', down_color='#ef5350',
        border_up_color='#26a69a', border_down_color='#ef5350',
        wick_up_color='#26a69a', wick_down_color='#ef5350'
    )

    chart.price_scale(align_labels=True, border_visible=False)
    chart.time_scale(border_visible=False, time_visible=True, seconds_visible=False)

    # Render Chart
    chart.load()

    # --- CUSTOM METRICS SECTION (HTML) ---
    change_color_class = "metric-change-pos" if change_abs >= 0 else "metric-change-neg"
    change_sign = "+" if change_abs >= 0 else ""

    st.markdown(f"""
    <div class="metrics-container">
        <div class="metric-item">
            <div class="metric-label">Candles</div>
            <div class="metric-value">{len(df)}</div>
        </div>
        <div class="metric-item">
            <div class="metric-label">Open</div>
            <div class="metric-value">${open_price:,.0f}</div>
        </div>
        <div class="metric-item">
            <div class="metric-label">High</div>
            <div class="metric-value">${high_price:,.0f}</div>
        </div>
        <div class="metric-item">
            <div class="metric-label">Low</div>
            <div class="metric-value">${low_price:,.0f}</div>
        </div>
        <div class="metric-item">
            <div class="metric-label">Close</div>
            <div class="metric-value">${close_price:,.0f}</div>
        </div>
        <div class="metric-item">
            <div class="metric-label">Change</div>
            <div class="metric-value">
                <span class="{change_color_class}">{change_sign}{change_pct:.2f}%</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)