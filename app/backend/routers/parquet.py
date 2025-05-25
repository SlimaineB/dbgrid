from fastapi import APIRouter, HTTPException, Request
from models.models import S3PathRequest, SuggestPartitionRequest, PartitionValueCountRequest
import os, psutil, re, json
from urllib.parse import unquote

router = APIRouter()


@router.post("/check_parquet_file_size")
def check_parquet_size(req: S3PathRequest, request: Request):
    con = request.app.state.con
    cpu_count = psutil.cpu_count(logical=True)

    try:
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

            file_data["parallelism_quality"] = (
                "✅ Optimal" if rg_count == cpu_count else
                "❌ Underutilized" if rg_count < cpu_count else
                "⚠️ Overhead Risk"
            )

            files.append(file_data)

        return {
            "s3_path": req.s3_path,
            "total_row_groups": total_row_groups,
            "cpu_count": cpu_count,
            "files": files
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/check_parquet_row_group_size")
def check_row_group_size(req: S3PathRequest, request: Request):
    con = request.app.state.con

    try:
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
        rows = con.execute(query).fetchall()
        columns = [desc[0] for desc in con.description]
        return {
            "s3_path": req.s3_path,
            "row_groups": [dict(zip(columns, row)) for row in rows]
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def extract_partition_columns_from_path(s3_path: str) -> set:
    decoded_path = unquote(s3_path)
    return set(re.findall(r'/([^/=]+)=', decoded_path))


@router.post("/suggest_partitions")
def suggest_partitions(req: SuggestPartitionRequest, request: Request):
    con = request.app.state.con

    try:
        con.execute(f"CREATE OR REPLACE VIEW parquet_data AS SELECT * FROM parquet_scan('{req.s3_path}');")
        existing_partitions = extract_partition_columns_from_path(req.s3_path)
        columns = con.execute("PRAGMA table_info(parquet_data);").fetchall()
        column_names = [col[1] for col in columns]

        suggestions = []
        result = []

        for col_name in column_names:
            already_partitioned = col_name in existing_partitions
            try:
                cardinality = con.execute(f"SELECT COUNT(DISTINCT {col_name}) FROM parquet_data;").fetchone()[0]
                top_val_ratio = con.execute(f"""
                    SELECT MAX(cnt) * 1.0 / SUM(cnt)
                    FROM (
                        SELECT COUNT(*) as cnt
                        FROM parquet_data
                        GROUP BY {col_name}
                    );
                """).fetchone()[0]
                is_balanced = top_val_ratio < 0.7

                if already_partitioned:
                    suggest = "🔁"
                elif cardinality <= req.threshold and is_balanced:
                    suggest = "✅"
                    suggestions.append(col_name)
                elif not is_balanced:
                    suggest = "⚠️ Unbalanced"
                else:
                    suggest = "❌"

                result.append({
                    "column": col_name,
                    "distinct_values": cardinality,
                    "top_value_ratio": round(top_val_ratio, 2),
                    "balanced": is_balanced,
                    "suggest": suggest,
                    "already_partitioned": already_partitioned
                })

            except Exception:
                result.append({
                    "column": col_name,
                    "distinct_values": "error",
                    "top_value_ratio": None,
                    "balanced": False,
                    "suggest": "⚠️",
                    "already_partitioned": already_partitioned
                })

        return {
            "s3_path": req.s3_path,
            "threshold": req.threshold,
            "already_partitioned_columns": list(existing_partitions),
            "columns": result,
            "suggested_partitions": suggestions
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/partition_value_counts")
def get_partition_value_counts(req: PartitionValueCountRequest, request: Request):
    con = request.app.state.con

    try:
        con.execute(f"CREATE OR REPLACE VIEW parquet_data AS SELECT * FROM parquet_scan('{req.s3_path}');")
        rows = con.execute(f"""
            SELECT {req.column} AS value, COUNT(*) AS count 
            FROM parquet_data 
            GROUP BY {req.column}
            ORDER BY count DESC
        """).fetchall()

        sum_count = sum(r[1] for r in rows)
        result = [
            {"value": r[0], "count": r[1], "repartion": f"{round(r[1] / sum_count * 100)}%"}
            for r in rows
        ]
        return {"counts": result}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/s3_test")
def test_s3_connection(request: Request):
    con = request.app.state.con
    try:
        con.execute("SELECT * FROM list('s3://your-bucket/') LIMIT 1;")
        return {"s3": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 config error: {e}")
