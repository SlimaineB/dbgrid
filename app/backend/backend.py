from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import duckdb
import os
import socket  # pour obtenir le hostname

app = FastAPI()

# Obtenir le nom d’hôte de la machine (utile en cluster)
hostname = socket.gethostname()
print(f"🖥️ Backend démarré sur le noeud : {hostname}")

# Connexion à DuckDB (embedded)
con = duckdb.connect(read_only = True)  # Connexion en lecture seule

# Charger l'extension httpfs si présente
ext_path = "/app/extensions/httpfs.duckdb_extension"
if os.path.isfile(ext_path):
    con.execute(f"LOAD '{ext_path}';")
else:
    print("⚠️ Warning: httpfs extension not found")

# Script d'initialisation custom au démarrage
init_script = os.getenv("INIT_SQL_PATH", "/app/init.sql")  # valeur par défaut
if os.path.isfile(init_script):
    print(f"📜 Executing init script: {init_script}")
    with open(init_script, "r") as f:
        con.execute(f.read())
else:
    print(f"ℹ️ No init script found at {init_script} — skipping.")

# Modèle pour la requête
class SQLRequest(BaseModel):
    query: str

# Endpoint principal
@app.post("/query")
def execute_query(req: SQLRequest):
    try:
        result = con.execute(req.query).fetchall()
        columns = [desc[0] for desc in con.description]
        return {
            "columns": columns,
            "rows": result,
            "hostname": hostname  # inclus le hostname dans la réponse
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
