from fastapi import APIRouter
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
