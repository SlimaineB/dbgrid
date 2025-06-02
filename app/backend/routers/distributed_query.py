import re
import os
import asyncio
import duckdb
import httpx
import logging
import time
import socket
import uuid
from collections import defaultdict
from fastapi import APIRouter, Request, HTTPException
from models.models import SQLRequest

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DISTRIBUTIVE_FUNCS = {"SUM", "COUNT", "AVG", "MIN", "MAX"}

def extract_aggregates(query: str) -> list:
    query_upper = re.sub(r'\n', ' ', query).upper()
    return list(set(re.findall(r'\b(SUM|COUNT|AVG|MIN|MAX)\s*\(', query_upper)))

def is_distributive_aggregation(query: str) -> bool:
    used = extract_aggregates(query)
    logger.info(f"🔍 Agrégats détectés : {used}")
    return all(agg in DISTRIBUTIVE_FUNCS for agg in used)

def extract_s3_path(query: str) -> str:
    match = re.search(r"read_parquet\(\s*'([^']+)'", query, re.IGNORECASE)
    if not match:
        raise HTTPException(400, "Impossible de détecter le chemin S3.")
    s3_path = match.group(1)
    logger.info(f"📂 Chemin S3 détecté : {s3_path}")
    return s3_path

def list_partitions(s3_path: str) -> tuple:
    con = duckdb.connect(database=':memory:')
    logger.info("📑 Lecture des chemins de fichiers S3 via DuckDB...")

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

    logger.info(f"📦 Partition détectée : {partition_col}, valeurs = {sorted(values_set)}")
    return partition_col, sorted(values_set)

def rewrite_query_for_partition(query: str, partition_col: str, value) -> str:
    condition = f"{partition_col} = '{value}'"
    if "where" in query.lower():
        return re.sub(r"(?i)\bWHERE\b", f"WHERE {condition} AND ", query, count=1)
    else:
        return re.sub(r"(?i)\bFROM\b\s+([^\s;]+)", f"FROM \\1 WHERE {condition}", query, count=1)

def merge_results(results: list, columns: list, aggregates: list) -> dict:
    logger.info("🧩 Fusion des résultats...")
    agg_map = {col.lower(): agg.upper() for agg, col in zip(aggregates, columns)}

    sum_map = defaultdict(float)
    count_map = defaultdict(int)
    min_map = {}
    max_map = {}

    for result in results:
        logger.info(f"Resultat reçu : {result}")
        rows = result.get("rows", [])
        if not rows:
            logger.warning("⚠️ Résultat vide, ignoré.")
            continue

        for row in rows:
            for idx, col in enumerate(columns):
                col_l = col.lower()
                val = row[idx]
                agg = agg_map.get(col_l)

                if val is None:
                    continue
                if agg == "SUM" or agg == "COUNT":
                    sum_map[col_l] += val
                    logger.info(f"Ajout de {val} à {col} ({agg})")

                #TODO: AVG should add count to be correct
                elif agg == "AVG":
                    sum_map[col_l] += val
                    count_map[col_l] += 1
                    logger.info(f"Ajout de {val} à {col} (AVG intermédiaire)")

                elif agg == "MIN":
                    min_map[col_l] = (
                        val if col_l not in min_map else min(min_map[col_l], val)
                    )
                    logger.info(f"MIN actuel de {col} : {min_map[col_l]}")

                elif agg == "MAX":
                    max_map[col_l] = (
                        val if col_l not in max_map else max(max_map[col_l], val)
                    )
                    logger.info(f"MAX actuel de {col} : {max_map[col_l]}")

    final_row = []
    for col in columns:
        col_l = col.lower()
        agg = agg_map.get(col_l)
        if agg == "SUM" or agg == "COUNT":
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

async def query_partition(lb_url: str, query: str, req: SQLRequest, partition: str, client: httpx.AsyncClient):
    logger.info(f"🚀 Requête envoyée pour la partition : {partition}")
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
        logger.info(f"✅ Réponse OK pour partition : {partition}")
        logger.info(f"Réponse de la partition {partition} : {response.text[:200]}...")  # Log only first 200 chars
        return response.json()
    except Exception as e:
        logger.error(f"❌ Erreur sur partition {partition} : {e}")
        raise HTTPException(500, f"Erreur sur partition '{partition}': {str(e)}")


@router.post("/distributed-query")
async def distributed_query(req: SQLRequest, request: Request):
    start_time = time.time()
    request_id = str(uuid.uuid4())
    logger.info(f"📥 [{request_id}] Nouvelle requête distribuée reçue")
    
    if not is_distributive_aggregation(req.query):
        logger.warning(f"⛔ [{request_id}] Agrégats non distributifs détectés. Abandon.")
        raise HTTPException(400, "La requête contient des agrégats non distributifs. Distribution impossible.")

    aggregates = extract_aggregates(req.query)
    s3_path = extract_s3_path(req.query)
    partition_col, values = list_partitions(s3_path)

    lb_url = req.lb_url
    if not lb_url:
        logger.critical(f"🚨 [{request_id}] lb_url non défini.")
        raise HTTPException(500, "lb_url non défini.")

    queries = [rewrite_query_for_partition(req.query, partition_col, val) for val in values]
    logger.info(f"📤 [{request_id}] {len(queries)} requêtes générées.")

    for q, val in zip(queries, values):
        logger.info(f"📨 Requête générée pour partition '{val}' :\n{q}")

    async with httpx.AsyncClient() as client:
        tasks = [
            query_partition(lb_url, q, req, val, client)
            for q, val in zip(queries, values)
        ]
        results = await asyncio.gather(*tasks)

    if not results:
        logger.error(f"😿 [{request_id}] Aucun résultat reçu.")
        raise HTTPException(500, "Aucun résultat retourné par les partitions.")

    columns = results[0]["columns"]
    merged = merge_results(results, columns, aggregates)
    exec_time = time.time() - start_time

    logger.info(f"✅ [{request_id}] Fusion terminée en {exec_time:.4f}s")

    return {
        "columns": merged["columns"],
        "rows": merged["rows"],
        "hostname": socket.gethostname(),
        "execution_time": exec_time,
        "partitions_used": merged["partitions_used"]
    }
