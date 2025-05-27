from fastapi import APIRouter, Request, HTTPException
from models.models import SQLRequest
import os, json, time, re, uuid, math
from decimal import Decimal
from datetime import datetime, date

router = APIRouter()

# 🔧 Fonction de sanitation de chaque valeur
def sanitize_value(val):
    if val is None:
        return None
    elif isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    elif isinstance(val, (int, str, bool)):
        return val
    elif isinstance(val, (Decimal, datetime, date)):
        return str(val)
    else:
        return str(val)  # fallback pour tout autre type non standard

# 🔧 Fonction de sanitation d'une ligne SQL
def sanitize_row(row):
    return [sanitize_value(col) for col in row]

@router.post("/query")
def execute_query(req: SQLRequest, request: Request):
    con = request.app.state.con
    hostname = os.uname().nodename

    try:
        query = req.query.strip().rstrip(';')

        # Ajoute LIMIT si c'est un SELECT sans LIMIT
        if re.match(r"(?i)^select\b", query) and not re.search(r"(?i)\blimit\b", query):
            query += f" LIMIT {req.max_rows}"

        if req.profiling:
            profile_path = f"/tmp/duckdb_profile_{uuid.uuid4().hex}.json"
            con.execute("SET enable_profiling = 'json'")
            con.execute(f"SET profiling_output = '{profile_path}'")

            if os.path.exists(profile_path):
                os.remove(profile_path)

            con.execute(query).fetchall()

            # Attend activement que le fichier soit écrit
            start = time.time()
            while not os.path.exists(profile_path):
                if time.time() - start > 2:
                    raise HTTPException(500, "Profiling file not written.")
                time.sleep(0.01)

            with open(profile_path) as f:
                profiling_data = json.load(f)

            os.remove(profile_path)  # nettoyage du fichier temporaire

            return {
                "profiling": profiling_data,
                "hostname": hostname
            }

        else:
            result = con.execute(query).fetchall()
            columns = [desc[0] for desc in con.description]
            sanitized_rows = [sanitize_row(row) for row in result]

            return {
                "columns": columns,
                "rows": sanitized_rows,
                "hostname": hostname
            }

    except Exception as e:
        print(f"Query execution failed: {e}")
        raise HTTPException(400, str(e))
