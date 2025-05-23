from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import duckdb

app = FastAPI()

# Connexion à DuckDB (embedded)
con = duckdb.connect()

class SQLRequest(BaseModel):
    query: str

@app.post("/query")
def execute_query(req: SQLRequest):
    try:
        result = con.execute(req.query).fetchall()
        columns = [desc[0] for desc in con.description]
        return {
            "columns": columns,
            "rows": result
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
