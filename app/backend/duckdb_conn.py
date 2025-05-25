import duckdb, os

def init_duckdb():
    con = duckdb.connect()
    ext_path = "/app/extensions/httpfs.duckdb_extension"
    if os.path.isfile(ext_path):
        con.execute(f"LOAD '{ext_path}';")
    init_script = os.getenv("INIT_SQL_PATH", "./init.sql")
    if os.path.isfile(init_script):
        with open(init_script) as f:
            con.execute(f.read())
    return con
