# filepath: ui/streamlit_app.py
"""
Senior Data Engineer & MLOps UI Dashboard:
Real-Time Interactive E-Commerce Cart & Hybrid Recommendation Interface.

Features:
- Live Health Check with Redis Feature Store & NCF Deep Learning status indicator.
- Interactive Session Cart: Add real products into active cart (calls POST /cart/add).
- Instant Real-Time Cross-Sell (calls GET /recommend/cart/{session_id} with < 5ms latency).
- Personalized Deep Learning Recommendations (calls GET /recommend/user/{user_id}).
- Real-Time Hybrid Ensemble (calls POST /recommend/hybrid with latency monitor).
"""

import ast
import json
import os
import pathlib
import time
from typing import Any, Dict, List

import pandas as pd
import requests
import streamlit as st

# Paths
ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
LAKEHOUSE_PARQUET = DATA_DIR / "lakehouse" / "real_transactions.parquet"
ONLINE_RETAIL_CSV = DATA_DIR / "online_retail.csv"
PRODUCTS_CSV = DATA_DIR / "products.csv"
RULES_JSON = DATA_DIR / "models" / "fpgrowth_rules.json"

# API Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="Real-Time Hybrid Recommender Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS Styling
css_path = pathlib.Path(__file__).parent / "style.css"
if css_path.exists():
    st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

# Custom Inline Badge Styles
st.markdown("""
<style>
.badge-green { background-color: #28a745; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; font-size: 12px; }
.badge-orange { background-color: #fd7e14; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; font-size: 12px; }
.badge-red { background-color: #dc3545; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; font-size: 12px; }
.metric-card { background: #f8f9fa; border: 1px solid #e9ecef; border-radius: 8px; padding: 12px; margin-bottom: 10px; }
.card-header { font-size: 14px; font-weight: 600; color: #495057; }
.card-val { font-size: 20px; font-weight: bold; color: #212529; }
</style>
""", unsafe_allow_html=True)


@st.cache_data(show_spinner=False)
def load_catalog_and_users():
    """Loads catalog metadata and list of valid users from Lakehouse Parquet or CSV."""
    if LAKEHOUSE_PARQUET.exists():
        df = pd.read_parquet(LAKEHOUSE_PARQUET)
    elif ONLINE_RETAIL_CSV.exists():
        df = pd.read_csv(ONLINE_RETAIL_CSV, encoding="ISO-8859-1")
    else:
        return pd.DataFrame(), []

    df.columns = [c.replace("\ufeff", "").strip().lower() for c in df.columns]
    u_col = "customerid" if "customerid" in df.columns else "customer id"
    i_col = "description" if "description" in df.columns else "item_name"
    p_col = "unitprice" if "unitprice" in df.columns else "price"
    c_col = "stockcode" if "stockcode" in df.columns else "item_id"

    df = df.dropna(subset=[u_col, i_col])
    df = df[df["quantity"] > 0]
    df[u_col] = df[u_col].astype(int)
    df[i_col] = df[i_col].astype(str).str.strip().str.lower()

    # User activity rankings
    user_counts = df[u_col].value_counts().reset_index()
    user_counts.columns = ["user_id", "orders_count"]

    # Catalog unique items
    catalog = df[[c_col, i_col, p_col]].drop_duplicates(subset=[i_col]).copy()
    catalog.columns = ["stockcode", "item_name", "price"]
    catalog["price"] = catalog["price"].astype(float)

    return catalog, user_counts


catalog_df, user_rank_df = load_catalog_and_users()


def fetch_api_health() -> Dict[str, Any]:
    try:
        r = requests.get(f"{API_URL}/health", timeout=3)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {"status": "offline", "redis_connected": False, "ncf_model_loaded": False}


health = fetch_api_health()

# ========================================================
# SIDEBAR: SYSTEM HEALTH & USER CONTEXT
# ========================================================
with st.sidebar:
    st.markdown("### ⚡ System Status")
    if health.get("status") in ["healthy", "ok"]:
        st.markdown('<span class="badge-green">API ONLINE</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="badge-red">API OFFLINE / DEGRADED</span>', unsafe_allow_html=True)

    r_conn = health.get("redis_connected", False)
    redis_badge = '<span class="badge-green">CONNECTED</span>' if r_conn else '<span class="badge-orange">IN-MEMORY FALLBACK</span>'
    st.markdown(f"**Redis Feature Store:** {redis_badge}", unsafe_allow_html=True)

    m_loaded = health.get("ncf_model_loaded", False)
    model_badge = '<span class="badge-green">READY</span>' if m_loaded else '<span class="badge-red">NOT LOADED</span>'
    st.markdown(f"**NCF Neural Model:** {model_badge}", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 👤 User Context")

    if not user_rank_df.empty:
        user_options = user_rank_df["user_id"].tolist()
        sel_user = st.selectbox("Select Customer ID", user_options, index=0)
        user_orders = int(user_rank_df[user_rank_df["user_id"] == sel_user]["orders_count"].iloc[0])
        st.caption(f"Historical Transactions: **{user_orders:,} items**")
    else:
        sel_user = st.number_input("Customer ID", value=17841, step=1)

    default_session = f"sess_{sel_user}_{int(time.time())}"
    if "session_id" not in st.session_state:
        st.session_state["session_id"] = default_session

    st.markdown("### 🛒 Active Session ID")
    st.session_state["session_id"] = st.text_input("Current Session ID", value=st.session_state["session_id"])

    top_k = st.slider("Top-K Recommendations", min_value=1, max_value=10, value=4)
    hybrid_alpha = st.slider("Hybrid Weight (Cart vs User)", min_value=0.0, max_value=1.0, value=0.6, step=0.1,
                             help="1.0 = Pure Cart intent (FP-Growth); 0.0 = Pure User profile (NCF)")


# ========================================================
# MAIN CONTENT: LIVE STREAMING E-COMMERCE CART
# ========================================================
st.markdown("## 🚀 Real-Time Hybrid Recommendation Platform")
st.caption("Powered by Apache Kafka Event Streaming, Redis Feature Store, FP-Growth Association Mining & PyTorch NCF.")

st.markdown("---")

col_cart_manage, col_cart_items = st.columns([1, 1])

with col_cart_manage:
    st.markdown("### ➕ Add Products to Active Cart")
    st.caption("Simulates live customer behavior (pushes event to Redis / Kafka pipeline).")

    if not catalog_df.empty:
        all_item_names = sorted(catalog_df["item_name"].tolist())
        selected_item_name = st.selectbox("Select a product to add", all_item_names)
        item_row = catalog_df[catalog_df["item_name"] == selected_item_name].iloc[0]
        item_price = float(item_row["price"])
        item_code = str(item_row["stockcode"])

        col_q, col_btn = st.columns([1, 2])
        with col_q:
            qty = st.number_input("Qty", min_value=1, max_value=10, value=1)
        with col_btn:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🛒 Add to Active Cart", use_container_width=True):
                # Call POST /cart/add
                try:
                    payload = {
                        "session_id": st.session_state["session_id"],
                        "user_id": int(sel_user),
                        "item_id": item_code,
                        "item_name": selected_item_name,
                        "price": item_price,
                        "quantity": int(qty),
                    }
                    resp = requests.post(f"{API_URL}/cart/add", json=payload, timeout=3)
                    if resp.status_code == 200:
                        st.success(f"Added '{selected_item_name}' (x{qty}) to live cart!")
                        time.sleep(0.3)
                        st.rerun()
                    else:
                        st.error(f"API Error: {resp.text}")
                except Exception as e:
                    st.error(f"Failed to communicate with API: {e}")

with col_cart_items:
    st.markdown(f"### 🛍️ Live Cart Content (`{st.session_state['session_id'][:18]}...`)")
    current_cart: Dict[str, Any] = {}
    try:
        c_resp = requests.get(f"{API_URL}/cart/{st.session_state['session_id']}", timeout=3)
        if c_resp.status_code == 200:
            current_cart = c_resp.json().get("items", {})
    except Exception:
        pass

    if current_cart:
        cart_rows = []
        for it_id, it_data in current_cart.items():
            cart_rows.append({
                "Stock Code": it_id,
                "Product Name": it_data.get("item_name", "").title(),
                "Unit Price": f"${float(it_data.get('price', 0.0)):.2f}",
                "Qty": it_data.get("quantity", 1),
            })
        st.dataframe(pd.DataFrame(cart_rows), use_container_width=True, hide_index=True)
    else:
        st.info("Your active cart is empty. Pick a product on the left to add it!")

st.markdown("---")

# ========================================================
# RECOMMENDATION PANELS (TABS)
# ========================================================
tab_live_cart, tab_ai_user, tab_hybrid = st.tabs([
    "⚡ Real-Time Cart Cross-Sell (FP-Growth)",
    "🧠 Personalized Profile (NCF Deep Learning)",
    "🔮 Hybrid Ensemble (Short-term + Long-term)",
])


def render_cards(items: List[Dict[str, Any]], title_key: str = "item_name", score_key: str = "score"):
    if not items:
        st.markdown("<em>No recommendations available for current signals.</em>", unsafe_allow_html=True)
        return

    cols = st.columns(min(len(items), 4))
    for idx, item in enumerate(items[:4]):
        with cols[idx % 4]:
            name = item.get(title_key, "").title()
            score = item.get(score_key, 0.0)
            lift = item.get("lift")
            trigger = item.get("triggered_by")

            st.markdown(f"""
            <div class="recommend-item" style="min-height: 140px;">
                <strong>{name}</strong><br>
                <span class="item-rank">Score / Confidence: {score:.4f}</span>
                {f'<br><small style="color: #6c757d;">Lift: {lift:.2f}</small>' if lift else ''}
                {f'<br><small style="color: #0d6efd;">Triggered by: {trigger.title()[:24]}</small>' if trigger else ''}
            </div>
            """, unsafe_allow_html=True)


# TAB 1: REAL-TIME CART (FP-GROWTH)
with tab_live_cart:
    st.markdown("#### ⚡ Sub-5ms Cross-Sell Recommendations")
    st.caption("Computed immediately from co-purchase association rules for items currently inside active cart.")

    if current_cart:
        try:
            t0 = time.perf_counter()
            r_cart = requests.get(f"{API_URL}/recommend/cart/{st.session_state['session_id']}?top_k={top_k}", timeout=3)
            cart_lat_ms = (time.perf_counter() - t0) * 1000
            if r_cart.status_code == 200:
                data = r_cart.json()
                st.markdown(f"**Query Latency:** `{cart_lat_ms:.2f} ms` | **Cart Items Count:** `{data.get('cart_items_count', 0)}`")
                render_cards(data.get("recommendations", []))
            else:
                st.error(f"Error fetching cart recs: {r_cart.text}")
        except Exception as e:
            st.error(f"Connection error: {e}")
    else:
        st.warning("Please add at least one item to the active cart above to trigger real-time cross-sell rules!")


# TAB 2: PERSONALIZED USER PROFILE (NCF)
with tab_ai_user:
    st.markdown(f"#### 🧠 Neural Collaborative Filtering for Customer `{sel_user}`")
    st.caption("Learned embeddings capturing customer long-term taste preferences across historical purchases.")

    try:
        t0 = time.perf_counter()
        r_user = requests.get(f"{API_URL}/recommend/user/{sel_user}?top_k={top_k}", timeout=3)
        user_lat_ms = (time.perf_counter() - t0) * 1000
        if r_user.status_code == 200:
            u_data = r_user.json()
            st.markdown(f"**Query Latency:** `{user_lat_ms:.2f} ms`")
            render_cards(u_data.get("recommendations", []))
        else:
            st.error(f"Error: {r_user.text}")
    except Exception as e:
        st.error(f"Connection error: {e}")


# TAB 3: HYBRID ENSEMBLE
with tab_hybrid:
    st.markdown("#### 🔮 Hybrid Ensemble Ranking")
    st.caption(f"Blends short-term cart intent ({hybrid_alpha*100:.0f}%) with long-term user profile ({(1-hybrid_alpha)*100:.0f}%).")

    try:
        t0 = time.perf_counter()
        h_payload = {
            "user_id": int(sel_user),
            "session_id": st.session_state["session_id"],
            "top_k": top_k,
            "alpha": hybrid_alpha,
        }
        r_hybrid = requests.post(f"{API_URL}/recommend/hybrid", json=h_payload, timeout=3)
        h_lat_ms = (time.perf_counter() - t0) * 1000
        if r_hybrid.status_code == 200:
            h_data = r_hybrid.json()
            st.markdown(f"**End-to-End Latency:** `{h_data.get('latency_ms', h_lat_ms):.2f} ms`")
            render_cards(h_data.get("recommendations", []), title_key="item_name", score_key="combined_score")
        else:
            st.error(f"Error: {r_hybrid.text}")
    except Exception as e:
        st.error(f"Connection error: {e}")

st.markdown("---")
st.caption("Data Platform Architecture: Apache Kafka • Redis Feature Store • PyTorch NCF • FP-Growth • Apache Airflow")
