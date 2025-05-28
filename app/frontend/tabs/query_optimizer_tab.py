import requests
from sqlglot import parse_one, optimizer, exp
import streamlit as st
import difflib

def extract_query_structure(tree):
    return {
        "Tables": [t.sql() for t in tree.find_all("Table")],
        "Joins": [j.sql() for j in tree.find_all("Join")],
        "Select Columns": [e.sql() for e in tree.expressions] if hasattr(tree, "expressions") else [],
        "Where": tree.args.get("where").sql() if tree.args.get("where") else "—",
        "Group By": tree.args.get("group").sql() if tree.args.get("group") else "—",
        "Having": tree.args.get("having").sql() if tree.args.get("having") else "—",
        "Order By": tree.args.get("order").sql() if tree.args.get("order") else "—",
        "Limit": tree.args.get("limit").sql() if tree.args.get("limit") else "—",
    }

def diff_explanation(sql1, sql2):
    d = difflib.unified_diff(
        sql1.splitlines(),
        sql2.splitlines(),
        fromfile="Original",
        tofile="Optimized",
        lineterm=""
    )
    return "\n".join(d)

def run_query_optimizer_tab(base_url: str, disable_ssl_verification: bool):
    st.header("🧠 SQL Optimizer")

    sql_input = st.text_area("📝 Original SQL query", height=300)

    if st.button("🔍 Optimize Query"):
        try:
            print("📥 Parsing SQL...")
            tree_original = parse_one(sql_input)
            print("✅ AST parsed:", tree_original)

            if not isinstance(tree_original, exp.Select):
                raise TypeError(f"Only SELECT queries are supported. Got: {type(tree_original)}")

            print("⚙️ Optimizing query...")
            tree_optimized = optimizer.optimize(tree_original)
            print("✅ Optimized AST:", tree_optimized)

            sql_original = tree_original.sql(pretty=True)
            sql_optimized = tree_optimized.sql(pretty=True)

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
            original = extract_query_structure(tree_original)
            optimized = extract_query_structure(tree_optimized)
            rows = []
            for key in original.keys():
                rows.append({
                    "Component": key,
                    "Original": original[key],
                    "Optimized": optimized[key],
                })
            st.dataframe(rows, use_container_width=True)

            st.subheader("⚙️ Execute and Compare")

            API_URL = base_url.rstrip("/") + "/query"
            payloads = [
                ("Original", sql_original),
                ("Optimized", sql_optimized)
            ]

            for label, query in payloads:
                with st.expander(f"▶️ {label} Execution Result", expanded=False):
                    try:
                        res = requests.post(API_URL, json={"sql": query}, verify=not disable_ssl_verification, timeout=10)
                        if res.ok:
                            data = res.json()
                            st.success(f"{label} executed successfully.")
                            st.write(f"⏱️ Time: {data.get('time', 'N/A')} ms")
                            st.dataframe(data.get("result", []))
                        else:
                            st.error(f"❌ {label} failed with status {res.status_code}")
                    except Exception as e:
                        st.error(f"❌ Error calling backend: {e}")

        except Exception as e:
            print(f"❌ [ERROR] {e}")
            st.error(f"❌ Error optimizing query: {e}")
