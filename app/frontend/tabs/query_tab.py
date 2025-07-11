import streamlit as st
import requests
import time
import pandas as pd

def run_query_tab(API_URL, disable_ssl_verification):
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
        "Enable Profiling": "PRAGMA enable_profiling = 'json';",
        "Select S3 Table":"SELECT * FROM read_parquet('s3://test-bucket/data/repartitionned/**/*.parquet') limit 5;",
        "Describe Table":"DESCRIBE SELECT * FROM read_parquet('s3://test-bucket/data/repartitionned/gender=F/timestamp*/is_promo=false/part-00051-ea7e82c4-3207-48aa-a37d-f9725c253166.c000.snappy.parquet');"
    }

    col1, col2 = st.columns([3, 1])
    with col2:
        st.markdown("#### Example Queries")
        example_choice = st.selectbox("", options=list(examples.keys()))
        st.markdown("#### Example SQL")
        st.code(examples[example_choice], language="sql")

        max_rows = st.selectbox("Maximum number of rows to display:", [10, 50, 100, 500, 1000], index=1)

      
        thread_mode = st.selectbox("Thread mode:", ["Default (Auto)", "Custom number of threads"])
        if thread_mode == "Custom number of threads":
            num_threads = st.number_input("Number of threads", min_value=1, step=1, value=2, max_value=200)
        else:
            num_threads = -1


    with col1:
        query = st.text_area("Your SQL query", height=250, value=examples[example_choice], placeholder="Ex: SELECT 1 as demo;")
        


        show_result_json = st.checkbox("Show SQL result as JSON", value=False)
        enable_profiling = st.checkbox("Enable profiling", value=False)

        if st.button("Execute query"):
            if not query.strip():
                st.warning("Please enter a query.")
                return

            try:
                start = time.time()
                payload = {
                    "query": query,
                    "profiling": enable_profiling,
                    "max_rows": max_rows,
                    "num_threads": num_threads  # 👈 value depends on mode
                }
                response = requests.post(API_URL, json=payload, verify=not disable_ssl_verification)
                elapsed = time.time() - start

                if response.status_code == 200:
                    st.success(f"✅ Executed in {elapsed:.4f} seconds")
                    data = response.json()

                    if "hostname" in data:
                        st.caption(f"📡 Served by: `{data['hostname']}`")

                    if "execution_time" in data:
                        st.caption(f"⏱️ Backend execution: `{data['execution_time']:.4f} sec`")

                    if "columns" in data and "rows" in data:
                        df = pd.DataFrame(data["rows"], columns=data["columns"])
                        st.dataframe(df, use_container_width=True)

                        if show_result_json:
                            result_json = [dict(zip(data["columns"], row)) for row in data["rows"]]
                            st.markdown("### SQL Result (JSON)")
                            st.json(result_json)

                    if enable_profiling and "profiling" in data:
                        st.markdown("### 🧪 Profiling JSON")
                        st.json(data["profiling"])

                else:
                    st.error(f"❌ Error: {response.json().get('detail', 'Unknown error')}")

            except Exception as e:
                st.error(f"🚫 Query failed: {e}")
