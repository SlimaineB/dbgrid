from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import duckdb
import os
import socket
import platform
import psutil
import json
import time
from typing import List
from collections import defaultdict

from urllib.parse import unquote
import re

app = FastAPI()

# Obtenir le nom d’hôte
hostname = socket.gethostname()
print(f"🖥️ Backend démarré sur le noeud : {hostname}")

# Connexion DuckDB
con = duckdb.connect()

# Charger httpfs si extension dispo
ext_path = "/app/extensions/httpfs.duckdb_extension"
if os.path.isfile(ext_path):
    con.execute(f"LOAD '{ext_path}';")
else:
    print("⚠️ Warning: httpfs extension not found")

# Script init si fourni
init_script = os.getenv("INIT_SQL_PATH", "/app/init.sql")
if os.path.isfile(init_script):
    print(f"📜 Executing init script: {init_script}")
    with open(init_script, "r") as f:
        con.execute(f.read())
else:
    print(f"ℹ️ No init script found at {init_script} — skipping.")

# ➕ Modèles Pydantic
class SQLRequest(BaseModel):
    query: str
    profiling: bool = False

class S3PathRequest(BaseModel):
    s3_path: str

class SuggestPartitionRequest(BaseModel):
    s3_path: str
    threshold: int = 10

class PartitionValueCountRequest(BaseModel):
    s3_path: str
    column: str

# 🔧 Fonction centralisée de config S3
def configure_s3():
    con.execute("SET s3_region='us-east-1'")
    con.execute("SET s3_url_style='path'")
    con.execute("SET s3_endpoint='localhost:9000'")
    con.execute("SET s3_access_key_id='minioadmin'")
    con.execute("SET s3_secret_access_key='minioadmin'")
    con.execute("SET s3_use_ssl=false")
    con.execute("INSTALL httpfs; LOAD httpfs;")


@app.post("/query")
def execute_query(req: SQLRequest):
    try:
        if req.profiling:
            profile_path = "/tmp/duckdb_profile.json"
            con.execute("SET enable_profiling = 'json';")
            con.execute(f"SET profiling_output = '{profile_path}';")

            if os.path.exists(profile_path):
                os.remove(profile_path)

            con.execute(req.query).fetchall()
            time.sleep(0.05)

            if os.path.exists(profile_path):
                with open(profile_path, "r") as f:
                    profile_data = json.load(f)
                return {
                    "profiling": profile_data,
                    "hostname": hostname
                }
            else:
                raise HTTPException(status_code=500, detail="Profiling file not written.")

        else:
            result = con.execute(req.query).fetchall()
            columns = [desc[0] for desc in con.description]
            return {
                "columns": columns,
                "rows": result,
                "hostname": hostname
            }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/status")
def get_status():
    try:
        return {
            "hostname": hostname,
            "os": platform.system(),
            "architecture": platform.machine(),
            "cpu_count": psutil.cpu_count(logical=True),
            "cpu_load": psutil.getloadavg(),
            "memory": {
                "total": psutil.virtual_memory().total,
                "available": psutil.virtual_memory().available,
                "used": psutil.virtual_memory().used,
                "percent": psutil.virtual_memory().percent
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Status error: {e}")

@app.post("/check_parquet_file_size")
def check_parquet_size(req: S3PathRequest):
    try:
        configure_s3()

        cpu_count = psutil.cpu_count(logical=True)

        query = f"""
            SELECT 
                file_name,
                COUNT(DISTINCT row_group_id) AS row_group_count,
                SUM(row_group_num_rows) AS total_rows,
                ROUND(SUM(total_compressed_size) / 1024.0 / 1024.0, 2) AS compressed_file_size_mb,
                ROUND(SUM(total_uncompressed_size) / 1024.0 / 1024.0, 2) AS uncompressed_file_size_mb,
                CASE
                    WHEN ROUND(SUM(total_compressed_size) / 1024.0 / 1024.0, 2) < 100 THEN 'Too small ❌'
                    WHEN ROUND(SUM(total_compressed_size) / 1024.0 / 1024.0, 2) > 10240 THEN 'Too big ⚠️'
                    ELSE 'Optimal ✅'
                END AS quality
            FROM parquet_metadata('{req.s3_path}')
            GROUP BY file_name
            ORDER BY compressed_file_size_mb DESC
        """

        result = con.execute(query).fetchall()
        columns = [desc[0] for desc in con.description]

        total_row_groups = sum(row[columns.index("row_group_count")] for row in result)

        files = []
        for row in result:
            file_data = dict(zip(columns, row))
            rg_count = file_data["row_group_count"]

            # 🔍 Parallelism Evaluation
            if rg_count < cpu_count:
                file_data["parallelism_quality"] = "❌ Underutilized"
            elif rg_count == cpu_count:
                file_data["parallelism_quality"] = "✅ Optimal"
            else:
                file_data["parallelism_quality"] = "⚠️ Overhead Risk"

            files.append(file_data)

        return {
            "s3_path": req.s3_path,
            "total_row_groups": total_row_groups,
            "cpu_count": cpu_count,
            "files": files
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))



@app.post("/check_parquet_row_group_size")
def check_row_group_size(req: S3PathRequest):
    try:
        configure_s3()
        query = f"""
            SELECT 
                file_name,
                row_group_id,
                row_group_num_rows,
                ROUND(row_group_bytes / 1024.0, 2) AS size_kb,
                CASE
                    WHEN row_group_num_rows < 5000 THEN 'Very small ❌'
                    WHEN row_group_num_rows < 20000 THEN 'Suboptimal ⚠️'
                    WHEN row_group_num_rows BETWEEN 100000 AND 1000000  THEN 'Optimal ✅'
                    WHEN row_group_num_rows >= 1000000 THEN 'Too high ⚠️'
                    ELSE 'okay'
                END AS quality
            FROM parquet_metadata('{req.s3_path}')
            GROUP BY file_name, row_group_id, row_group_num_rows, row_group_bytes
            ORDER BY size_kb DESC
        """
        result = con.execute(query).fetchall()
        columns = [desc[0] for desc in con.description]
        return {
            "s3_path": req.s3_path,
            "row_groups": [dict(zip(columns, row)) for row in result]
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))



def extract_partition_columns_from_path(s3_path: str) -> set:
    """Extrait les noms de colonnes déjà utilisées comme partition dans le chemin S3"""
    decoded_path = unquote(s3_path)
    return set(re.findall(r'/([^/=]+)=', decoded_path))

@app.post("/suggest_partitions")
def suggest_partitions(req: SuggestPartitionRequest):
    try:
        configure_s3()
        con.execute(f"CREATE OR REPLACE VIEW parquet_data AS SELECT * FROM parquet_scan('{req.s3_path}');")

        existing_partitions = extract_partition_columns_from_path(req.s3_path)

        columns = con.execute("PRAGMA table_info(parquet_data);").fetchall()
        column_names = [col[1] for col in columns]

        result = []
        suggestions = []

        for col_name in column_names:
            already_partitioned = col_name in existing_partitions

            try:
                cardinality = con.execute(f"SELECT COUNT(DISTINCT {col_name}) FROM parquet_data;").fetchone()[0]

                # Ratio de la valeur la plus fréquente
                top_val_ratio = con.execute(f"""
                    SELECT MAX(cnt) * 1.0 / SUM(cnt)
                    FROM (
                        SELECT COUNT(*) as cnt
                        FROM parquet_data
                        GROUP BY {col_name}
                    );
                """).fetchone()[0]

                is_balanced = top_val_ratio < 0.7  # configurable : seuil max 70% pour dire "équilibré"

                if already_partitioned:
                    suggest = "🔁"
                elif cardinality <= req.threshold and is_balanced:
                    suggest = "✅"
                    suggestions.append(col_name)
                elif not is_balanced:
                    suggest = "⚠️ Unbalanced"
                else:
                    suggest = "❌"

                result.append({
                    "column": col_name,
                    "distinct_values": cardinality,
                    "top_value_ratio": round(top_val_ratio, 2),
                    "balanced": is_balanced,
                    "suggest": suggest,
                    "already_partitioned": already_partitioned
                })

            except Exception:
                result.append({
                    "column": col_name,
                    "distinct_values": "error",
                    "top_value_ratio": None,
                    "balanced": False,
                    "suggest": "⚠️",
                    "already_partitioned": already_partitioned
                })

        return {
            "s3_path": req.s3_path,
            "threshold": req.threshold,
            "already_partitioned_columns": list(existing_partitions),
            "columns": result,
            "suggested_partitions": suggestions
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))



@app.post("/partition_value_counts")
def get_partition_value_counts(req: PartitionValueCountRequest):
    try:
        configure_s3()
        con.execute(f"CREATE OR REPLACE VIEW parquet_data AS SELECT * FROM parquet_scan('{req.s3_path}');")
        rows = con.execute(f"""
            SELECT {req.column} AS value, COUNT(*) AS count 
            FROM parquet_data 
            GROUP BY {req.column}
            ORDER BY count DESC
        """).fetchall()

        result = [{"value": r[0], "count": r[1]} for r in rows]
        sum_count = sum( r['count'] for r in result)
        result = [{"value": r[0], "count": r[1],"repartion": f"{round(r[1]/sum_count*100)}%"} for r in rows]
        return {"counts": result}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/s3_test")
def test_s3_connection():
    try:
        configure_s3()
        con.execute("SELECT * FROM list('s3://your-bucket/') LIMIT 1;")
        return {"s3": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 config error: {e}")
