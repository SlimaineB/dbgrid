import streamlit as st
import requests
import pandas as pd
import time

def run_tuning_tab(base_url: str, disable_ssl_verification: bool):

  
    st.subheader("🎯 Parquet Tuning for GridDB")

    with st.expander("ℹ️ Best Practices (DuckDB)"):
        st.markdown("""
        - ✅ **Parquet File Size**: Optimal between **100 MB** and **10 GB** per file.
        - ✅ **Row Groups**: Each group should contain **at least 100,000 rows**.
        - ⚠️ Row groups with **<5,000 rows** cause **5–10× slower** performance.
        - 💡 For **full parallelism**, aim for as many row groups as CPU threads.
        """)

    # Input: S3 path
    s3_path = st.text_input("📂 S3 Path to Parquet File:", "s3://test-bucket/data/init/**/*.parquet")

    if not s3_path.startswith("s3://"):
        st.warning("Please enter a valid S3 path (starting with s3://)")
        return

    # Analysis options
    check_parquet_size = st.checkbox("Check Parquet File Size")
    check_row_group_size = st.checkbox("Check Row Group Size")

    if st.button("Run Analysis"):
        if not (check_parquet_size or check_row_group_size):
            st.info("Please select at least one analysis to run.")
            return

        payload = {"s3_path": s3_path}

        if check_parquet_size:
            st.markdown("### 📦 Parquet File Size")
            try:
                resp = requests.post(f"{base_url}/check_parquet_file_size", json=payload, verify=not disable_ssl_verification)

                if resp.status_code == 200:
                    data = resp.json()
                    st.success("File size analysis successful")

                    # Résumé
                    st.markdown(f"**Total Row Groups:** {data['total_row_groups']}")
                    st.markdown(f"**CPU Threads (backend):** {data['cpu_count']}")
                    st.markdown(f"**Number of Files:** {len(data['files'])}")

                    # DataFrame
                    df = pd.DataFrame(data["files"])
                    df.rename(columns={
                        "file_name": "File Name",
                        "row_group_count": "Row Groups",
                        "total_rows": "Total Rows",
                        "compressed_file_size_mb": "Compressed Size (MB)",
                        "uncompressed_file_size_mb": "Uncompressed Size (MB)",
                        "quality": "Size Quality",
                        "parallelism_quality": "Parallelism Quality"
                    }, inplace=True)

                    st.dataframe(df)

                    with st.expander("ℹ️ Best Practices for Parquet File Size"):
                        st.markdown("""
                        - ✅ Optimal file size: **100 MB to 10 GB** per file  
                        - ✅ Prefer multiple files to leverage parallelism  
                        - ⚠️ Too small files degrade performance  
                        - ✅ **Total row groups ≥ number of CPU threads**
                        - 💡 Parallelism by file is optimal when **Row Groups = CPU Threads**
                        """)
                else:
                    st.error("Error during file size check.")
                    st.text(resp.text)
            except Exception as e:
                st.error(f"Request error: {e}")

        if check_row_group_size:
            st.markdown("### 📊 Parquet Row Group Size")
            try:
                resp = requests.post(f"{base_url}/check_parquet_row_group_size", json=payload, verify=not disable_ssl_verification)

                if resp.status_code == 200:
                    data = resp.json()
                    st.success("Row group analysis successful")

                    df = pd.DataFrame(data["row_groups"])
                    df.rename(columns={
                        "file_name": "File Name",
                        "row_group_id": "Row Group ID",
                        "row_group_num_rows": "Num Rows",
                        "size_kb": "Size (KB)",
                        "quality": "Quality"
                    }, inplace=True)

                    st.dataframe(df)

                    with st.expander("ℹ️ Best Practices for Row Groups"):
                        st.markdown("""
                        - ✅ Aim for **100,000–1,000,000 rows per group**  
                        - ❌ <5,000 rows: inefficient (vectorization fails)  
                        - ⚠️ >1,000,000 rows: might be too large for memory  
                        """)
                else:
                    st.error("Error during row group size check.")
                    st.text(resp.text)
            except Exception as e:
                st.error(f"Request error: {e}")
