import os
import re
import time
import uuid
import socket
import asyncio
import logging
import duckdb
import httpx

from collections import defaultdict
from fastapi import APIRouter, Request, HTTPException
from models.models import SQLRequest
from sqlglot import parse_one, exp
from sqlglot.expressions import Select, EQ, Column, Literal
import sqlglot

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DISTRIBUTIVE_FUNCS = {"SUM", "COUNT", "AVG", "MIN", "MAX"}

# --- SQL Helpers ---

def extract_aggregates(query: str) -> list:
    expression = parse_one(query)
    aggs = set()
    for agg in expression.find_all(exp.AggFunc):
        aggs.add(agg.__class__.__name__.upper())
    return list(aggs)

def is_distributive_aggregation(query: str) -> bool:
    used = extract_aggregates(query)
    logger.info(f"Agrégats détectés : {used}")
    return all(agg in DISTRIBUTIVE_FUNCS for agg in used)

def extract_s3_path(query: str) -> str:
    tree = parse_one(query)
    table = tree.find(exp.Table)
    if not table:
        raise HTTPException(400, "Impossible de détecter la table FROM.")

    func = table.this
    if not isinstance(func, exp.Anonymous):
        raise HTTPException(400, "FROM n'appelle pas une fonction.")

    if func.name.lower() != "read_parquet":
        raise HTTPException(400, "FROM n'utilise pas read_parquet.")

    arg = func.expressions[0]
    if not isinstance(arg, exp.Literal):
        raise HTTPException(400, "Le chemin S3 n'est pas un littéral.")

    s3_path = arg.this
    logger.info(f"Chemin S3 détecté : {s3_path}")
    return s3_path

from sqlglot.expressions import Select, EQ, Column, Literal, Func, And

def inject_partition_condition(expr: exp.Expression, partition_col: str, value: str) -> exp.Expression:
    """
    Injecte 'partition_col = value' dans chaque SELECT contenant un FROM avec read_parquet().
    """
    for select_expr in expr.find_all(Select):
        logger.info(f"⛏ Traitement SELECT: {select_expr.sql()}")
        from_expr = select_expr.args.get("from")

        if not from_expr or not from_expr.expressions:
            logger.warning("⚠️ Pas de FROM dans ce SELECT")
            continue

        for table_expr in from_expr.expressions:
            func_expr = getattr(table_expr, "this", None)
            if isinstance(func_expr, Func) and func_expr.name.lower() == "read_parquet":
                logger.info("✅ read_parquet() détecté")
                condition = EQ(this=Column(this=partition_col), expression=Literal.string(value))
                existing_where = select_expr.args.get("where")
                if existing_where:
                    combined = And(this=condition, expression=existing_where)
                    select_expr.set("where", combined)
                else:
                    select_expr.set("where", condition)

    return expr




def rewrite_query_for_partition(query: str, partition_col: str, value: str) -> str:
    parsed = sqlglot.parse_one(query)
    modified = inject_partition_condition(parsed, partition_col, value)
    return modified.sql(pretty=False)

# --- S3 Helpers ---

def list_partitions(s3_path: str) -> tuple:
    con = duckdb.connect(database=':memory:')
    logger.info("Lecture des fichiers parquet...")

    try:
        df = con.execute(f"""
            SELECT DISTINCT filename 
            FROM read_parquet('{s3_path}', filename=True)
        """).fetchdf()
    except Exception as e:
        raise HTTPException(400, f"Erreur lecture Parquet : {str(e)}")

    if df.empty:
        raise HTTPException(400, "Aucun fichier trouvé dans le chemin S3.")

    filenames = df['filename'].tolist()
    partition_col = None
    values_set = set()

    for path in filenames:
        match = re.search(r"/([^/=]+)=([^/]+)/", path)
        if match:
            col, val = match.groups()
            partition_col = col
            values_set.add(val)

    if not partition_col or not values_set:
        raise HTTPException(400, "Impossible de détecter les partitions depuis les chemins S3.")

    logger.info(f"Partition détectée : {partition_col}, valeurs = {sorted(values_set)}")
    return partition_col, sorted(values_set)

# --- Fusion des résultats ---

def merge_results(results: list, columns: list, aggregates: list) -> dict:
    logger.info("Fusion des résultats...")
    agg_map = {}
    for col in columns:
        col_l = col.lower()
        if col_l.startswith("sum("):
            agg_map[col_l] = "SUM"
        elif col_l.startswith("avg("):
            agg_map[col_l] = "AVG"
        elif col_l.startswith("min("):
            agg_map[col_l] = "MIN"
        elif col_l.startswith("max("):
            agg_map[col_l] = "MAX"
        elif col_l.startswith("count("):
            agg_map[col_l] = "COUNT"

    sum_map = defaultdict(float)
    count_map = defaultdict(int)
    min_map = {}
    max_map = {}

    for result in results:
        logger.info(f"Résultat reçu : {result}")
        rows = result.get("rows", [])
        if not rows:
            logger.warning("Résultat vide, ignoré.")
            continue

        for row in rows:
            for idx, col in enumerate(columns):
                col_l = col.lower()
                val = row[idx]
                agg = agg_map.get(col_l)

                if val is None:
                    continue
                if agg in {"SUM", "COUNT"}:
                    sum_map[col_l] += val
                elif agg == "AVG":
                    sum_map[col_l] += val
                    count_map[col_l] += 1
                elif agg == "MIN":
                    min_map[col_l] = val if col_l not in min_map else min(min_map[col_l], val)
                elif agg == "MAX":
                    max_map[col_l] = val if col_l not in max_map else max(max_map[col_l], val)

    final_row = []
    for col in columns:
        col_l = col.lower()
        agg = agg_map.get(col_l)
        if agg in {"SUM", "COUNT"}:
            final_row.append(sum_map.get(col_l, 0))
        elif agg == "AVG":
            count = count_map.get(col_l, 0)
            final_row.append(sum_map.get(col_l, 0) / count if count else None)
        elif agg == "MIN":
            final_row.append(min_map.get(col_l))
        elif agg == "MAX":
            final_row.append(max_map.get(col_l))
        else:
            final_row.append(None)

    return {
        "columns": columns,
        "rows": [final_row],
        "partitions_used": len(results)
    }

# --- Async HTTP ---

async def query_partition(lb_url: str, query: str, req: SQLRequest, partition: str, client: httpx.AsyncClient):
    logger.info(f"Requête envoyée pour la partition : {partition}")
    try:
        response = await client.post(
            f"{lb_url}/query",
            json={
                "query": query,
                "profiling": req.profiling,
                "max_rows": req.max_rows,
                "num_threads": req.num_threads
            },
            timeout=20.0
        )
        response.raise_for_status()
        logger.info(f"Réponse OK pour partition : {partition}")
        return response.json()
    except Exception as e:
        logger.error(f"Erreur sur partition {partition} : {e}")
        raise HTTPException(500, f"Erreur sur partition '{partition}': {str(e)}")

# --- Main Endpoint ---

@router.post("/distributed-query")
async def distributed_query(req: SQLRequest, request: Request):
    start_time = time.time()
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Nouvelle requête distribuée reçue")

    if not is_distributive_aggregation(req.query):
        logger.warning(f"[{request_id}] Agrégats non distributifs détectés. Abandon.")
        raise HTTPException(400, "La requête contient des agrégats non distributifs. Distribution impossible.")

    aggregates = extract_aggregates(req.query)
    s3_path = extract_s3_path(req.query)
    partition_col, values = list_partitions(s3_path)

    lb_url = req.lb_url
    if not lb_url:
        logger.critical(f"[{request_id}] lb_url non défini.")
        raise HTTPException(500, "lb_url non défini.")

    queries = [rewrite_query_for_partition(req.query, partition_col, val) for val in values]
    logger.info(f"[{request_id}] {len(queries)} requêtes générées.")

    async with httpx.AsyncClient() as client:
        tasks = [query_partition(lb_url, q, req, val, client) for q, val in zip(queries, values)]
        results = await asyncio.gather(*tasks)

    if not results:
        logger.error(f"[{request_id}] Aucun résultat reçu.")
        raise HTTPException(500, "Aucun résultat retourné par les partitions.")

    columns = results[0]["columns"]
    merged = merge_results(results, columns, aggregates)
    exec_time = time.time() - start_time

    logger.info(f"[{request_id}] Fusion terminée en {exec_time:.4f}s")

    return {
        "columns": merged["columns"],
        "rows": merged["rows"],
        "hostname": socket.gethostname(),
        "execution_time": exec_time,
        "partitions_used": merged["partitions_used"]
    }
