from fastapi import APIRouter, Request, HTTPException
from models.models import SQLRequest
import os, json, time, re, uuid, math, logging
from decimal import Decimal
from datetime import datetime, date

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
        return str(val)

def sanitize_row(row):
    return [sanitize_value(col) for col in row]

@router.post("/query")
def execute_query(req: SQLRequest, request: Request):
    gloabal_con = request.app.state.con

    con = gloabal_con.cursor() # Local connexion for this request
    hostname = os.uname().nodename

    original_threads = None
    start_time = time.time()

    logger.info(f"📥 Received query from {hostname}")
    logger.info(f"🧵 Threads requested: {req.num_threads}")
    logger.info(f"📝 Query:\n{req.query.strip()}")

    try:
        query = req.query.strip().rstrip(';')

        if req.num_threads != -1:
            try:
                original_threads = con.execute("SELECT current_setting('threads') AS val").fetchone()[0]
                con.execute(f"SET threads TO {req.num_threads}")
                logger.info(f"✅ Threads set to {req.num_threads} (original was {original_threads})")
            except Exception as e:
                logger.warning(f"⚠️ Failed to set threads: {e}")

        if re.match(r"(?i)^select\b", query) and not re.search(r"(?i)\blimit\b", query):
            query += f" LIMIT {req.max_rows}"
            logger.info(f"➕ Appended LIMIT {req.max_rows}")

        if req.profiling:
            profile_path = f"/tmp/duckdb_profile_{uuid.uuid4().hex}.json"
            con.execute("SET enable_profiling = 'json'")
            con.execute(f"SET profiling_output = '{profile_path}'")

            if os.path.exists(profile_path):
                os.remove(profile_path)

            con.execute(query).fetchall()

            start_wait = time.time()
            while not os.path.exists(profile_path):
                if time.time() - start_wait > 2:
                    raise HTTPException(500, "Profiling file not written.")
                time.sleep(0.01)

            with open(profile_path) as f:
                profiling_data = json.load(f)

            os.remove(profile_path)

            exec_time = time.time() - start_time
            logger.info(f"📈 Profiling completed in {exec_time:.4f} seconds")

            return {
                "profiling": profiling_data,
                "hostname": hostname,
                "execution_time": exec_time
            }

        else:
            result = con.execute(query).fetchall()
            columns = [desc[0] for desc in con.description]
            sanitized_rows = [sanitize_row(row) for row in result]

            exec_time = time.time() - start_time
            logger.info(f"📊 Returned {len(sanitized_rows)} rows in {exec_time:.4f} seconds")

            return {
                "columns": columns,
                "rows": sanitized_rows,
                "hostname": hostname,
                "execution_time": exec_time
            }

    except Exception as e:
        logger.error(f"❌ Query execution failed: {e}")
        raise HTTPException(400, str(e))

    finally:
        if original_threads is not None:
            try:
                con.execute(f"SET threads TO {original_threads}")
                logger.info(f"🔄 Threads reset to original value: {original_threads}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to reset threads to {original_threads}: {e}")
