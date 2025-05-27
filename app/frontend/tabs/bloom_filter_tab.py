import streamlit as st
import requests
import pandas as pd

def run_bloom_filter_tab(base_url: str, disable_ssl_verification: bool):
    st.title("🔍 Parquet Bloom & Filterability Analysis")

    s3_path = st.text_input("📂 S3 Path to Parquet File", 
        "s3://test-bucket/data/repartitionned/gender=F/timestamp=2025-05-24 16%3A26%3A03.278/is_promo=false/part-00051-ea7e82c4-3207-48aa-a37d-f9725c253166.c000.snappy.parquet"
    )

    if not s3_path.startswith("s3://"):
        st.warning("❗ Please enter a valid S3 path starting with s3://")
        return

    tab1, tab2 = st.tabs(["📊 Filterability Score", "🧪 Bloom Filter Presence"])

    # --- Tab 1: Filterability Score ---
    with tab1:
        st.subheader("📊 Column Filterability Score")
        st.markdown("""
        This tool analyzes your Parquet file and shows:
        - Presence of **Bloom Filters**
        - **Cardinality** of each column
        - **Dominance of top values**
        - A final **Filterability Score** (0–3)

        Use this to identify columns that are efficient for **predicate pushdown**.
        """)

        if st.button("🔍 Analyze Filterability"):
            try:
                with st.spinner("⏳ Running filterability analysis..."):
                    resp = requests.post(
                        f"{base_url}/parquet_filterability_score",
                        json={"s3_path": s3_path},
                        verify=not disable_ssl_verification
                    )

                if resp.status_code == 200:
                    data = resp.json()
                    df = pd.DataFrame(data["columns"])

                    df["top_value_ratio"] = (df["top_value_ratio"] * 100).round(1).astype(str) + "%"
                    df["bloom_filter_coverage_percent"] = df["bloom_filter_coverage_percent"].astype(str) + "%"

                    st.success("✅ Filterability analysis complete")
                    st.dataframe(df[[ 
                        "column", 
                        "distinct_values", 
                        "top_value_ratio", 
                        "bloom_filter_coverage_percent", 
                        "filterability_score",
                        "filterability_label"
                    ]].sort_values("filterability_score", ascending=False), use_container_width=True)

                    st.markdown("""### 🧠 Scoring Logic
- `+1` if column has Bloom Filters  
- `+1` if cardinality > 50  
- `+1` if top value ratio < 50%  
""")
                else:
                    st.error("❌ Backend error:")
                    st.text(resp.text)

            except Exception as e:
                st.error(f"Request failed: {e}")

    # --- Tab 2: Bloom Filter Presence ---
    with tab2:
        st.subheader("🧪 Row Group Bloom Filter Presence")
        st.markdown("This tab checks actual bloom filter storage in each row group for each column.")

        if st.button("🧬 Check Bloom Filter Presence"):
            try:
                with st.spinner("⏳ Checking bloom filters..."):
                    resp = requests.post(
                        f"{base_url}/parquet_bloom_filter_check",
                        json={"s3_path": s3_path},
                        verify=not disable_ssl_verification
                    )

                if resp.status_code == 200:
                    data = resp.json()
                    df = pd.DataFrame(data["columns"])

                    with st.expander("🔍 Filter"):
                        min_presence = st.slider("Minimum Bloom Filter Presence (%)", 0, 100, 0)
                        statuses = st.multiselect("Status Filter", df["status"].unique(), default=[])

                        if min_presence > 0:
                            df = df[df["presence_ratio"] >= min_presence]
                        if statuses:
                            df = df[df["status"].isin(statuses)]

                    st.success(f"✅ Loaded {len(df)} column-file pairs.")
                    st.dataframe(df, use_container_width=True)

                    st.markdown("### 📈 Average Bloom Presence by Column")
                    chart_df = (
                        df.groupby("column")["presence_ratio"]
                        .mean()
                        .sort_values(ascending=False)
                        .reset_index()
                        .rename(columns={"presence_ratio": "avg_presence_%"})
                    )
                    st.bar_chart(chart_df.set_index("column"))
                else:
                    st.error("❌ Backend error:")
                    st.text(resp.text)

            except Exception as e:
                st.error(f"Request failed: {e}")
