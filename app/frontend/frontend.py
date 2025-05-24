import streamlit as st
import os
from query_page import run_query_page
from cluster_page import run_cluster_page

st.set_page_config(page_title="DuckDB Client", layout="wide")

# Bandeau en haut
st.markdown("""
<div style="background-color:#0e1117;padding:1rem;border-radius:10px;margin-bottom:2rem;">
    <h2 style="color:white;margin:0;">🧠 GridDB Frontend</h2>
    <p style="color:#bbb;margin:0;">Enter an SQL query, execute it via the GridDB backend, and view the results.</p>
</div>
""", unsafe_allow_html=True)

# Sidebar pour config backend
default_base_url = os.getenv("BACKEND_URL", "http://localhost:8000")
backend_base_url = st.sidebar.text_input("Backend base URL (without /query):", value=default_base_url)
disable_ssl_verification = False  # toujours False pour simplicité

API_URL = backend_base_url.rstrip("/") + "/query"
STATUS_URL = backend_base_url.rstrip("/") + "/status"

# Onglets en haut
tabs = st.tabs(["Query", "Cluster"])

with tabs[0]:
    run_query_page(API_URL, disable_ssl_verification)

with tabs[1]:
    run_cluster_page(STATUS_URL, disable_ssl_verification)
