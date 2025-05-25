from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import duckdb
import os
import socket
import platform
import psutil
import json
import time

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



@app.get("/s3_test")
def test_s3_connection():
    try:
        configure_s3()
        con.execute("SELECT * FROM list('s3://your-bucket/') LIMIT 1;")
        return {"s3": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 config error: {e}")
