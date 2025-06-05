from fastapi import FastAPI
from routers import query, distributed_query, query_analyzer, status, parquet
from duckdb_conn import init_duckdb

app = FastAPI()

# Initialise DuckDB + extensions + init.sql
con = init_duckdb()
app.state.con = con


# Inclure les routers
app.include_router(query.router)
app.include_router(distributed_query.router)
app.include_router(query_analyzer.router)
app.include_router(status.router)
app.include_router(parquet.router)
