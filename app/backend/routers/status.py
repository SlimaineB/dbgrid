from fastapi import APIRouter, Request, HTTPException
import platform, psutil, socket

router = APIRouter()

@router.get("/status")
def get_status():
    try:
        return {
            "hostname": socket.gethostname(),
            "os": platform.system(),
            "architecture": platform.machine(),
            "cpu_count": psutil.cpu_count(logical=True),
            "cpu_load": psutil.getloadavg(),
            "memory": dict(psutil.virtual_memory()._asdict())
        }
    except Exception as e:
        raise HTTPException(500, f"Status error: {e}")


@router.get("/live")
def liveness_check():
    return {"status": "alive"}


@router.get("/ready")
def readiness_check(request: Request):
    try:
        # Vérifie si DuckDB fonctionne
        con = request.app.state.con.cursor()
        con.execute("SELECT 1")
        
        # Vérifie si le cache est accessible (lecture simple)
        cache_dir = "./db_cache"
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
        
        test_file = os.path.join(cache_dir, "readiness_check.tmp")
        with open(test_file, "w") as f:
            f.write("ok")
        os.remove(test_file)

        return {"status": "ready", "duckdb": "ok", "disk": "ok"}

    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Not ready")
