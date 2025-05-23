import streamlit as st
import requests
import time
import pandas as pd

API_URL = "http://localhost:8000/query"  # Make sure the backend is running here

st.set_page_config(page_title="DuckDB Client", layout="wide")

# Styled banner
st.markdown("""
<div style="background-color:#0e1117;padding:1rem;border-radius:10px;margin-bottom:2rem;">
    <h2 style="color:white;margin:0;">🧠 GridDB Frontend </h2>
    <p style="color:#bbb;margin:0;">Enter an SQL query, execute it via the FastAPI backend, and view the results.</p>
</div>
""", unsafe_allow_html=True)

query = st.text_area("Your SQL query", height=150, placeholder="Ex: SELECT 1 as demo;")

# Selector for maximum number of rows to display
max_rows = st.selectbox("Maximum number of rows to display:", [10, 50, 100, 500, 1000], index=1)

if st.button("Execute query"):
    if not query.strip():
        st.warning("Please enter a query.")
    else:
        try:
            start = time.time()
            response = requests.post(API_URL, json={"query": query})
            elapsed = time.time() - start

            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data["rows"], columns=data["columns"])
                st.success(f"✅ Executed in {elapsed:.4f} seconds")
                st.dataframe(df.head(max_rows), use_container_width=True)
            else:
                st.error(f"❌ Error: {response.json()['detail']}")
        except Exception as e:
            st.error(f"🚫 Query failed: {e}")
