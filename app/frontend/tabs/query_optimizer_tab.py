import requests
import streamlit as st
import difflib

def diff_explanation(sql1, sql2):
    d = difflib.unified_diff(
        sql1.splitlines(),
        sql2.splitlines(),
        fromfile="Original",
        tofile="Optimized",
        lineterm=""
    )
    return "\n".join(d)

def format_components_inline(components: dict) -> str:
    lines = []
    for key, value in components.items():
        if isinstance(value, list):
            value_str = ", ".join(value) if value else "—"
        else:
            value_str = value or "—"
        lines.append(f"**{key}:** `{value_str}`")
    return "\n\n".join(lines)

def fetch_execution_plan(query_url: str, disable_ssl_verification: bool, query, label):
    res = requests.post(
        query_url,
        json={"query": query, "profiling": True, "max_rows": 1, "num_threads": -1},
        verify=not disable_ssl_verification,
        timeout=10,
    )
    if res.ok:
        plan = res.json().get("profiling", "")
        st.subheader(f"🗺️ Execution Plan - {label}")
        st.json(plan)
    else:
        st.error(f"Failed to fetch execution plan for {label}")

def run_query_optimizer_tab(base_url: str, disable_ssl_verification: bool):
    st.header("🧠 SQL Optimizer")

    sql_input = st.text_area("📝 Original SQL query", height=300)

    if st.button("🔍 Optimize Query"):
        try:
            ANALYZE_URL = base_url.rstrip("/") + "/analyze"
            QUERY_URL = base_url.rstrip("/") + "/query"

            res = requests.post(ANALYZE_URL, json={"sql": sql_input}, verify=not disable_ssl_verification, timeout=10)
            if not res.ok:
                st.error(f"❌ Backend error: {res.status_code}")
                return

            data = res.json()
            if "error" in data:
                st.error(f"❌ Analyze error: {data['error']}")
                return

            sql_original = data["original_sql"]
            sql_optimized = data["optimized_sql"]

            col1, col2 = st.columns(2)
            with col1:
                st.subheader("📥 Original Query")
                st.code(sql_original, language="sql")
            with col2:
                st.subheader("📈 Optimized Query")
                st.code(sql_optimized, language="sql")

            if sql_original.strip() != sql_optimized.strip():
                st.subheader("🧾 Changes Detected")
                st.code(diff_explanation(sql_original, sql_optimized), language="diff")
                st.info("✅ SQLGlot applied syntactic optimizations.")
            else:
                st.success("✅ Query already optimal.")

            st.subheader("📋 Query Components")
            st.markdown("### Original")
            st.markdown(format_components_inline(data["components"]))
            st.markdown("### Optimized")
            st.markdown(format_components_inline(data["components_optimized"]))

            st.subheader("⚙️ Execute and Compare")
            payloads = [
                ("Original", sql_original),
                ("Optimized", sql_optimized)
            ]

            for label, query in payloads:
                with st.expander(f"▶️ {label} Execution Result", expanded=False):
                    try:
                        res = requests.post(
                            QUERY_URL,
                            json={
                                "query": query,
                                "profiling": False,
                                "max_rows": 50,
                                "num_threads": -1
                            },
                            verify=not disable_ssl_verification,
                            timeout=10
                        )
                        if res.ok:
                            data = res.json()
                            st.success(f"{label} executed successfully.")
                            st.write(f"⏱️ Time: {round(data.get('execution_time'),3) if data.get('execution_time') else 'N/A'} s")
                            if "columns" in data and "rows" in data:
                                st.dataframe([dict(zip(data["columns"], row)) for row in data["rows"]])
                            elif "profiling" in data:
                                st.json(data["profiling"])
                            else:
                                st.write(data)
                        else:
                            st.error(f"❌ {label} failed with status {res.status_code}")
                    except Exception as e:
                        st.error(f"❌ Error calling backend: {e}")

            fetch_execution_plan(QUERY_URL, disable_ssl_verification,sql_original, "Original Query")
            fetch_execution_plan(QUERY_URL,disable_ssl_verification,sql_optimized, "Optimized Query")
        except Exception as e:
            st.error(f"❌ Unexpected error: {e}")
