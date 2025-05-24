import streamlit as st
import requests
import time
import pandas as pd
import os

st.set_page_config(page_title="DuckDB Client", layout="wide")

# Backend config and SSL option
default_base_url = os.getenv("BACKEND_URL", "http://localhost:8000")
backend_base_url = st.text_input("Backend base URL (without /query):", value=default_base_url)
disable_ssl_verification = st.checkbox("Disable SSL verification (not recommended)", value=False)
API_URL = backend_base_url.rstrip("/") + "/query"

# Styled banner
st.markdown("""
<div style="background-color:#0e1117;padding:1rem;border-radius:10px;margin-bottom:2rem;">
    <h2 style="color:white;margin:0;">🧠 GridDB Frontend</h2>
    <p style="color:#bbb;margin:0;">Enter an SQL query, execute it via the FastAPI backend, and view the results.</p>
</div>
""", unsafe_allow_html=True)

# Example queries
examples = {
    "Select simple constants": "SELECT 1 AS id, 'hello' AS message;",
    "Select sample data": """
        SELECT * FROM (VALUES
            (1, 'Alice'),
            (2, 'Bob'),
            (3, 'Charlie')
        ) AS t(id, name);
    """,
    "Update example (no results)": "UPDATE my_table SET col1 = 'value' WHERE id = 1;",
    "Show Settings": "SELECT * FROM duckdb_settings();",
    "Enable  Profiling": "PRAGMA enable_profiling = 'json';",
}

# Layout with two columns
col1, col2 = st.columns([3, 1])

with col2:
    st.markdown("### Example Queries")
    example_choice = st.selectbox("", options=list(examples.keys()))
    st.markdown("#### Example SQL")
    st.code(examples[example_choice], language="sql")

with col1:
    query = st.text_area("Your SQL query", height=250, value=examples[example_choice], placeholder="Ex: SELECT 1 as demo;")
    max_rows = st.selectbox("Maximum number of rows to display:", [10, 50, 100, 500, 1000], index=1)
    show_result_json = st.checkbox("Show SQL result as JSON", value=False)
    enable_profiling = st.checkbox("Enable profiling", value=False)

    if st.button("Execute query"):
        if not query.strip():
            st.warning("Please enter a query.")
        else:
            try:
                start = time.time()
                payload = {"query": query, "profiling": enable_profiling}
                response = requests.post(API_URL, json=payload, verify=not disable_ssl_verification)
                elapsed = time.time() - start

                if response.status_code == 200:
                    data = response.json()

                    # Display table
                    if "columns" in data and "rows" in data:
                        df = pd.DataFrame(data["rows"], columns=data["columns"])
                        st.dataframe(df.head(max_rows), use_container_width=True)

                        if show_result_json:
                            result_json = [dict(zip(data["columns"], row)) for row in data["rows"]]
                            st.markdown("### SQL Result (JSON)")
                            st.json(result_json)

                    # Profiling JSON if enabled
                    if enable_profiling and "profiling" in data:
                        st.markdown("### 🧪 Profiling JSON")
                        st.json(data["profiling"])

                    # Backend info
                    if "hostname" in data:
                        st.caption(f"📡 Served by: `{data['hostname']}`")

                    st.success(f"✅ Executed in {elapsed:.4f} seconds")
                else:
                    st.error(f"❌ Error: {response.json()['detail']}")
            except Exception as e:
                st.error(f"🚫 Query failed: {e}")
