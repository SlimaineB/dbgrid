from fastapi import APIRouter, Request, HTTPException
from models.models import SQLRequest
import os, json, time

router = APIRouter()

@router.post("/query")
def execute_query(req: SQLRequest, request: Request):
    con = request.app.state.con
    hostname = os.uname().nodename

    try:
        if req.profiling:
            profile_path = "/tmp/duckdb_profile.json"
            con.execute("SET enable_profiling = 'json'")
            con.execute(f"SET profiling_output = '{profile_path}'")
            if os.path.exists(profile_path):
                os.remove(profile_path)
            con.execute(req.query).fetchall()
            time.sleep(0.05)

            if os.path.exists(profile_path):
                with open(profile_path) as f:
                    return {
                        "profiling": json.load(f),
                        "hostname": hostname
                    }
            else:
                raise HTTPException(500, "Profiling file not written.")

        else:
            result = con.execute(req.query).fetchall()
            columns = [desc[0] for desc in con.description]
            return {"columns": columns, "rows": result, "hostname": hostname}

    except Exception as e:
        raise HTTPException(400, str(e))
