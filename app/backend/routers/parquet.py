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


@router.post("/parquet_filterability_score")
def parquet_filterability_score(req: S3PathRequest, request: Request):
    con = request.app.state.con

    try:
        s3_path = req.s3_path
        con.execute(f"CREATE OR REPLACE VIEW parquet_data AS SELECT * FROM parquet_scan('{s3_path}')")

        # 1. Cardinality and top value ratio
        cols = con.execute("PRAGMA table_info(parquet_data);").fetchall()
        col_names = [col[1] for col in cols]

        results = []
        for col in col_names:
            try:
                distinct_count = con.execute(f"SELECT COUNT(DISTINCT {col}) FROM parquet_data").fetchone()[0]
                top_val_ratio = con.execute(f"""
                    SELECT MAX(cnt) * 1.0 / SUM(cnt)
                    FROM (
                        SELECT COUNT(*) AS cnt
                        FROM parquet_data
                        GROUP BY {col}
                    )
                """).fetchone()[0] or 0.0

                results.append({
                    "column": col,
                    "distinct_values": distinct_count,
                    "top_value_ratio": round(top_val_ratio, 2),
                })

            except Exception:
                results.append({
                    "column": col,
                    "distinct_values": None,
                    "top_value_ratio": None,
                })

        # 2. Bloom filter metadata
        bf_rows = con.execute(f"""
            SELECT 
                path_in_schema AS column,
                COUNT(*) AS num_row_groups,

                SUM(CASE 
                    WHEN bloom_filter_offset IS NOT NULL AND bloom_filter_length > 0 THEN 1 
                    ELSE 0 
                END) AS num_with_bloom,

                SUM(CASE 
                    WHEN bloom_filter_offset IS NOT NULL AND bloom_filter_length = 0 THEN 1 
                    ELSE 0 
                END) AS num_declared_but_empty,

                SUM(CASE 
                    WHEN bloom_filter_offset IS NOT NULL AND bloom_filter_length IS NULL THEN 1 
                    ELSE 0 
                END) AS num_declared_but_length_missing,

                ROUND(SUM(CASE 
                    WHEN bloom_filter_offset IS NOT NULL AND bloom_filter_length > 0 THEN 1 
                    ELSE 0 
                END) * 100.0 / COUNT(*), 1) AS bloom_coverage

            FROM parquet_metadata('{s3_path}')
            WHERE path_in_schema IS NOT NULL
            GROUP BY path_in_schema
        """).fetchall()

        bf_info = {
            row[0]: {
                "num_row_groups": row[1],
                "num_with_bloom": row[2],
                "num_declared_but_empty": row[3],
                "num_declared_but_length_missing": row[4],
                "bloom_coverage": row[5],
            } for row in bf_rows
        }

        # 3. Merge & Score
        for r in results:
            col = r["column"]
            bloom_data = bf_info.get(col, {})
            coverage = bloom_data.get("bloom_coverage", 0.0)
            cardinality = r.get("distinct_values") or 0
            top_ratio = r.get("top_value_ratio") or 1.0

            score = 0
            if coverage > 0:
                score += 1
            if cardinality > 50:
                score += 1
            if top_ratio < 0.5:
                score += 1

            r.update({
                "bloom_filter_coverage_percent": coverage,
                "row_groups_with_bloom": bloom_data.get("num_with_bloom", 0),
                "row_groups_declared_but_empty": bloom_data.get("num_declared_but_empty", 0),
                "row_groups_declared_but_length_missing": bloom_data.get("num_declared_but_length_missing", 0),
                "filterability_score": score,
                "filterability_label": (
                    "✅ High" if score == 3 else
                    "🟡 Medium" if score == 2 else
                    "⚠️ Low"
                )
            })

        return {
            "s3_path": s3_path,
            "columns": results
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/parquet_bloom_filter_check")
def check_bloom_filter(req: S3PathRequest, request: Request):
    con = request.app.state.con

    try:
        query = f"""
                SELECT 
                    file_name,
                    row_group_id,
                    path_in_schema AS column,
                    bloom_filter_offset IS NOT NULL AND bloom_filter_length > 0 AS has_bloom_filter,
                    bloom_filter_offset,
                    bloom_filter_length,
                    CASE 
                        WHEN bloom_filter_offset IS NOT NULL THEN 
                            CASE 
                                WHEN bloom_filter_length > 0 THEN '✅ Present'
                                ELSE '⚠️ Declared but empty'
                            END
                        ELSE '❌ Absent'
                    END AS status
                FROM parquet_metadata('{req.s3_path}')
                WHERE path_in_schema IS NOT NULL
                ORDER BY file_name, path_in_schema;
        """

        rows = con.execute(query).fetchall()
        columns = [desc[0] for desc in con.description]

        # Regroup by file/column
        grouped = {}
        for row in rows:
            row_dict = dict(zip(columns, row))
            file = row_dict["file_name"]
            col = row_dict["column"]
            status = row_dict["status"]

            grouped.setdefault((file, col), []).append(status)

        summary = []
        for (file, col), statuses in grouped.items():
            present_ratio = statuses.count("✅ Present") / len(statuses)
            summary.append({
                "file": file,
                "column": col,
                "num_row_groups": len(statuses),
                "num_with_bloom": statuses.count("✅ Present"),
                "num_declared_but_empty": statuses.count("⚠️ Declared but empty"),
                "num_absent": statuses.count("❌ Absent"),
                "presence_ratio": round(statuses.count("✅ Present") * 100 / len(statuses), 1),
                "declared_but_empty_ratio": round(statuses.count("⚠️ Declared but empty") * 100 / len(statuses), 1),
                "status": (
                    "✅ Fully Present" if statuses.count("✅ Present") == len(statuses) else
                    "⚠️ Some Empty" if statuses.count("⚠️ Declared but empty") > 0 and statuses.count("✅ Present") > 0 else
                    "⚠️ Declared but Empty" if statuses.count("⚠️ Declared but empty") == len(statuses) else
                    "❌ Absent"
                )
            })


        return {
            "s3_path": req.s3_path,
            "columns": summary
        }

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
