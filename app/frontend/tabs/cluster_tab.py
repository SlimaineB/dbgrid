import streamlit as st
import requests
import pandas as pd

def ping_status_multiple_times(status_url, n=10, disable_ssl=False):
    collected_statuses = {}
    errors = []
    for i in range(n):
        try:
            resp = requests.get(status_url, verify=not disable_ssl, timeout=3)
            if resp.status_code == 200:
                data = resp.json()
                node_name = data.get("hostname", f"unknown_{i}")
                collected_statuses[node_name] = data
            else:
                errors.append(f"Attempt {i+1}: HTTP {resp.status_code}")
        except Exception as e:
            errors.append(f"Attempt {i+1}: Exception {str(e)}")
    return collected_statuses, errors

def run_cluster_tab(STATUS_URL, disable_ssl_verification):
    st.markdown("## Cluster Node Status")
    num_pings = st.slider("Number of status pings to send:", min_value=1, max_value=50, value=10)
    if st.button("Get Backend Status"):
        with st.spinner(f"Pinging {num_pings} times to gather cluster info..."):
            statuses, errors = ping_status_multiple_times(STATUS_URL, n=num_pings, disable_ssl=disable_ssl_verification)
        
        if statuses:
            st.success(f"Got status from {len(statuses)} unique node(s).")

            # Préparer un dataframe résumé avec cpu total et ram total
            rows = []
            total_cpu = 0
            total_mem = 0
            for node, status in statuses.items():
                mem = status.get('memory', {})
                cpu_load_1m = status.get('cpu_load', [0,0,0])[0] if status.get('cpu_load') else 0
                cpu_count = status.get('cpu_count', 0)
                mem_total = mem.get('total', 0)
                total_cpu += cpu_count
                total_mem += mem_total

                rows.append({
                    "Node": node,
                    "CPU Count (total cores)": cpu_count,
                    "CPU Load (1m avg)": cpu_load_1m,
                    "Memory Used (%)": mem.get('percent', 0),
                    "Memory Total (GB)": mem_total / (1024**3),
                    "Memory Used (GB)": mem.get('used', 0) / (1024**3),
                })
            df_summary = pd.DataFrame(rows)

            # Afficher les totaux cluster
            st.markdown(f"### Total CPU cores: **{total_cpu}**")
            st.markdown(f"### Total Memory: **{total_mem / (1024**3):.2f} GB**")

            # Afficher tableau résumé enrichi
            st.markdown("### Summary Table")
            st.dataframe(df_summary)

            # Graphique CPU Load
            st.markdown("### CPU Load (1 minute average) per Node")
            st.bar_chart(df_summary.set_index("Node")["CPU Load (1m avg)"])

            # Graphique mémoire utilisée en %
            st.markdown("### Memory Used (%) per Node")
            st.bar_chart(df_summary.set_index("Node")["Memory Used (%)"])

            st.markdown("---")

            # Détail complet par noeud (existant)
            for node, status in statuses.items():
                st.markdown(f"### Node: `{node}`")
                st.json(status)
                st.markdown(f"**OS:** {status.get('os')} - **Architecture:** {status.get('architecture')}")
                st.markdown(f"**CPU count (logical):** {status.get('cpu_count')}")
                st.markdown(f"**CPU Load (1m,5m,15m):** {status.get('cpu_load')}")
                mem = status.get('memory', {})
                st.markdown(f"**Memory Total:** {mem.get('total',0) // (1024**2)} MB")
                st.markdown(f"**Memory Used:** {mem.get('used',0) // (1024**2)} MB ({mem.get('percent',0)}%)")
                st.markdown(f"**Memory Available:** {mem.get('available',0) // (1024**2)} MB")
                st.markdown("---")

        else:
            st.error("No status retrieved from any node.")

        if errors:
            st.warning(f"Errors / failed attempts ({len(errors)}):")
            for err in errors:
                st.write(err)
