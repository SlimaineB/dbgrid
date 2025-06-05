from fastapi import APIRouter, Request, HTTPException
from models.models import SQLRequest, CacheRequest
import os, json, time, re, uuid, math, logging
import hashlib
import threading
import duckdb

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
    global_con = request.app.state.con

    con = global_con #.cursor() # Local connexion for this request
    hostname = os.uname().nodename

    original_threads = None
    start_time = time.time()

    logger.info(f"Received query from {hostname}")
    logger.info(f"Threads requested: {req.num_threads}")
    logger.info(f"Query:\n{req.query.strip()}")

    try:
        query = req.query.strip().rstrip(';')

        if req.num_threads != -1:
            try:
                original_threads = con.execute("SELECT current_setting('threads') AS val").fetchone()[0]
                con.execute(f"SET threads TO {req.num_threads}")
                logger.info(f"Threads set to {req.num_threads} (original was {original_threads})")
            except Exception as e:
                logger.warning(f"Failed to set threads: {e}")

        if re.match(r"(?i)^select\b", query) and not re.search(r"(?i)\blimit\b", query):
            query += f" LIMIT {req.max_rows}"
            logger.info(f"Appended LIMIT {req.max_rows}")

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
            logger.info(f"Profiling completed in {exec_time:.4f} seconds")

            return {
                "profiling": profiling_data,
                "hostname": hostname,
                "execution_time": exec_time
            }

        else:
            result = execute_db_query(con, query, req.force_refresh_cache)
            columns = [desc[0] for desc in con.description]
            sanitized_rows = [sanitize_row(row) for row in result]

            exec_time = time.time() - start_time
            logger.info(f"Returned {len(sanitized_rows)} rows in {exec_time:.4f} seconds")

            return {
                "columns": columns,
                "rows": sanitized_rows,
                "hostname": hostname,
                "execution_time": exec_time
            }

    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(400, str(e))

    finally:
        if original_threads is not None:
            try:
                con.execute(f"SET threads TO {original_threads}")
                logger.info(f"Threads reset to original value: {original_threads}")
            except Exception as e:
                logger.warning(f"Failed to reset threads to {original_threads}: {e}")




def perform_cache(con, query: str, output_path: str):
    try:
        logger.info("Performing synchronous caching task...")

        is_s3 = "://" in output_path
        if not is_s3:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

        logger.info(f"Caching query to: {output_path}")
        cache_sql = f"""
            COPY (
                SELECT subq.*, NOW() AS cached_at
                FROM ({query}) AS subq
            ) TO '{output_path}'
            (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
        """

        con.execute(cache_sql)
        logger.info(f"Cached result to {output_path}")

    except Exception as e:
        logger.warning(f"Failed to cache query: {e}")



def execute_db_query(con, query, force_refresh_cache: bool = False):
    CACHE_OUTPUT_BASE = os.getenv("CACHE_OUTPUT_BASE", "./db_cache")
    MAX_CACHE_AGE_MINUTES = int(os.getenv("CACHE_TTL_MINUTES", "60"))

    normalized_query = query.strip().rstrip(';')
    query_hash = hashlib.sha256(normalized_query.encode('utf-8')).hexdigest()
    cached_date = str(date.today())
    parquet_path = f"{CACHE_OUTPUT_BASE.rstrip('/')}/cached_date={cached_date}/db_cache_{query_hash}.parquet"

    logger.info(f"Using cache base: {CACHE_OUTPUT_BASE}")

    try:
        if not force_refresh_cache:
            try:
                logger.info(f"Attempting to read cache from {parquet_path}")
                result = con.execute(
                    f"""
                    SELECT * EXCLUDE (cached_at, cached_date)
                    FROM read_parquet('{parquet_path}')
                    WHERE cached_at >= NOW() - INTERVAL '{MAX_CACHE_AGE_MINUTES} minutes'
                    """
                ).fetchall()
                logger.info(f"Using cached result from {parquet_path}")
                return result
            except Exception:
                logger.info(f"No valid cache found or failed to read at {parquet_path}")

        start_time = time.time()
        result = con.execute(normalized_query).fetchall()
        duration = time.time() - start_time

        if duration > 0.5:
            perform_cache(con, normalized_query, parquet_path)

        return result

    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise



