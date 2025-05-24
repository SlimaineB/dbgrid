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

# Extension HTTPFS
ext_path = "/app/extensions/httpfs.duckdb_extension"
if os.path.isfile(ext_path):
    con.execute(f"LOAD '{ext_path}';")
else:
    print("⚠️ Warning: httpfs extension not found")

# Script init
init_script = os.getenv("INIT_SQL_PATH", "/app/init.sql")
if os.path.isfile(init_script):
    print(f"📜 Executing init script: {init_script}")
    with open(init_script, "r") as f:
        con.execute(f.read())
else:
    print(f"ℹ️ No init script found at {init_script} — skipping.")

# ➕ Modèle avec option profiling
class SQLRequest(BaseModel):
    query: str
    profiling: bool = False



@app.post("/query")
def execute_query(req: SQLRequest):
    try:
        if req.profiling:
            profile_path = "/tmp/duckdb_profile.json"
            # Activer profiling JSON et définir fichier de sortie
            con.execute("SET enable_profiling = 'json';")
            con.execute(f"SET profiling_output = '{profile_path}';")

            # Supprimer ancien profil s’il existe
            if os.path.exists(profile_path):
                os.remove(profile_path)

            # Exécuter la requête
            con.execute(req.query).fetchall()

            # Attendre que DuckDB écrive le fichier
            time.sleep(0.05)

            # Lire et retourner le contenu JSON
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
            # Requête simple sans profiling
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
