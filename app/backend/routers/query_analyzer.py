from fastapi import APIRouter, Request, HTTPException
from models.models import SQLAnalyzerRequest
from pydantic import BaseModel
from sqlglot import parse_one, optimizer, exp
import sqlite3
import time

router = APIRouter()



def extract_expr(expr):
    return expr.this.sql() if expr else "—"

def extract_group_by(group_expr):
    if not group_expr:
        return "—"
    def render(e):
        if isinstance(e, exp.GroupingSets):
            sets = [f"({', '.join(sub.sql() for sub in g.expressions)})" for g in e.expressions]
            return f"GROUPING SETS ({', '.join(sets)})"
        elif isinstance(e, exp.Cube):
            return f"CUBE({', '.join(sub.sql() for sub in e.expressions)})"
        elif isinstance(e, exp.Rollup):
            return f"ROLLUP({', '.join(sub.sql() for sub in e.expressions)})"
        return e.sql()
    return ", ".join(render(e) for e in group_expr.expressions)

def extract_query_components(tree: exp.Expression):
    return {
        "Tables": [t.sql() for t in tree.find_all(exp.Table)],
        "Projections": [s.sql() for s in tree.expressions] if hasattr(tree, "expressions") else [],
        "Where": extract_expr(tree.args.get("where")),
        "Group By": extract_group_by(tree.args.get("group")),
        "Having": extract_expr(tree.args.get("having")),
        "Order By": ", ".join(e.sql() for e in tree.args.get("order").expressions) if tree.args.get("order") else "—",
        "Limit": tree.args.get("limit").sql() if tree.args.get("limit") else "—"
    }

# --- Endpoints

@router.post("/analyze")
def analyze_sql(req: SQLAnalyzerRequest, request: Request):
    try:
        tree = parse_one(req.sql)
        tree_optimized = optimizer.optimize(tree)

        return {
            "original_sql": tree.sql(pretty=True),
            "optimized_sql": tree_optimized.sql(pretty=True),
            "components": extract_query_components(tree),
            "components_optimized": extract_query_components(tree_optimized)
        }
    except Exception as e:
        return {"error": str(e)}

