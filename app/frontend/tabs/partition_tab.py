import streamlit as st
import requests
import pandas as pd

def run_partition_tab(base_url: str, disable_ssl_verification: bool):
    st.subheader("🧩 Partition Recommendation")

    st.markdown("This tool helps identify which columns are good candidates for **partitioning** based on their cardinality and value distribution.")

    s3_path = st.text_input("📂 S3 Path to Parquet Files", 
        "s3://test-bucket/data/repartitionned/gender=F/timestamp=2025-05-24 16%3A26%3A03.278/is_promo=false/part-00051-ea7e82c4-3207-48aa-a37d-f9725c253166.c000.snappy.parquet"
    )

    threshold = st.slider("🔢 Max DISTINCT values to consider for partitioning", min_value=2, max_value=10, value=5)
    show_value_distribution = st.checkbox("🔎 Show value distribution for suggested partitions")

    if st.button("Analyze Columns"):
        if not s3_path.startswith("s3://"):
            st.warning("Please provide a valid S3 path.")
            return

        try:
            with st.spinner("🚀 Analyzing columns... please wait"):
                resp = requests.post(
                    f"{base_url}/suggest_partitions",
                    json={"s3_path": s3_path, "threshold": threshold},
                    verify=not disable_ssl_verification
                )

            if resp.status_code == 200:
                data = resp.json()
                suggested = data["suggested_partitions"]
                already_partitioned = data.get("already_partitioned_columns", [])

                df = pd.DataFrame(data["columns"])
                df["distinct_values"] = pd.to_numeric(df["distinct_values"], errors='coerce')
                df["most_frequent_percent"] = (df["top_value_ratio"] * 100).round(1).astype(str) + "%"

                df = df.sort_values("distinct_values", na_position="last")

                st.markdown(f"### 🔍 Column Analysis (Threshold: ≤ {threshold})")
                st.dataframe(df[[ 
                    "column", 
                    "distinct_values", 
                    "most_frequent_percent", 
                    "balanced", 
                    "already_partitioned", 
                    "suggest" 
                ]])

                st.markdown("---")

                if already_partitioned:
                    st.info(f"🔁 Already Partitioned Columns: `{', '.join(already_partitioned)}`")

                if suggested:
                    st.success(f"✅ Suggested Partition Columns: `{', '.join(suggested)}`")

                    st.markdown("### 📦 Recommended Partitioning Strategy")
                    st.markdown(f"""
                    The following columns are recommended for partitioning, **in priority order**:

                    **`{ ' → '.join(suggested) }`**

                    ⚠️ Avoid over-partitioning: prefer 1-3 columns max, especially if later ones are skewed.
                    """)

                    st.markdown("### 📌 Conclusion")
                    st.markdown("""
                        Columns were selected based on low cardinality and balanced distribution.  
                        Prioritize those at the top of the list when nesting partitions to reduce query skew and cost.
                    """)

                    if show_value_distribution:
                        for col in suggested:
                            st.markdown(f"#### 🔢 Value counts for `{col}`")
                            try:
                                with st.spinner(f"📊 Fetching value distribution for `{col}`..."):
                                    value_resp = requests.post(
                                        f"{base_url}/partition_value_counts",
                                        json={"s3_path": s3_path, "column": col},
                                        verify=not disable_ssl_verification
                                    )
                                if value_resp.status_code == 200:
                                    count_df = pd.DataFrame(value_resp.json()["counts"])
                                    st.dataframe(count_df)
                                else:
                                    st.warning(f"⚠️ Failed to fetch value counts for {col}")
                            except Exception as e:
                                st.error(f"Request error for {col}: {e}")
                else:
                    st.warning("⚠️ No columns meet the criteria for good partitioning at this threshold.")

            else:
                st.error("❌ Backend error.")
                st.text(resp.text)

        except Exception as e:
            st.error(f"Request error: {e}")
