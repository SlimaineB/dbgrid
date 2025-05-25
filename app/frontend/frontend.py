import streamlit as st
import os
import requests
from pages.query_page import run_query_page
from pages.cluster_page import run_cluster_page
from pages.tuning_page import run_tuning_page
from pages.partition_page import run_partition_page

st.set_page_config(page_title="DuckDB Client", layout="wide")

# Bandeau en haut
st.markdown("""
<div style="background-color:#0e1117;padding:1rem;border-radius:10px;margin-bottom:2rem;">
    <h2 style="color:white;margin:0;">🧠 GridDB Frontend</h2>
    <p style="color:#bbb;margin:0;">Enter an SQL query, execute it via the GridDB backend, and view the results.</p>
</div>
""", unsafe_allow_html=True)

# Sidebar - backend URL
default_base_url = os.getenv("BACKEND_URL", "http://localhost:8000")
backend_base_url = st.sidebar.text_input("🔗 Backend base URL (without /query):", value=default_base_url)
disable_ssl_verification = st.sidebar.checkbox("Disable SSL Verification", value=False)

# Sidebar - S3 config
st.sidebar.markdown("---")
st.sidebar.markdown("### 🪣 S3 Configuration")

# Test backend status
API_URL = backend_base_url.rstrip("/") + "/query"
STATUS_URL = backend_base_url.rstrip("/") + "/status"
TUNING_BASE_URL = backend_base_url.rstrip("/")

try:
    resp = requests.get(STATUS_URL, verify=not disable_ssl_verification, timeout=2)
    if resp.ok:
        st.sidebar.success("✅ Backend is up")
    else:
        st.sidebar.warning("⚠️ Backend might be unreachable")
except Exception as e:
    st.sidebar.error(f"❌ Error reaching backend: {e}")

# Onglets
tabs = st.tabs([
    "🧪 Run Query",
    "📊 Cluster",
    "⚙️ Tuning",
    "🧩 Partitioning"
])


with tabs[0]:
    run_query_page(API_URL, disable_ssl_verification)

with tabs[1]:
    run_cluster_page(STATUS_URL, disable_ssl_verification)

with tabs[2]:
    run_tuning_page(TUNING_BASE_URL, disable_ssl_verification)

with tabs[3]:
    run_partition_page(
        TUNING_BASE_URL,
        disable_ssl_verification
    )
