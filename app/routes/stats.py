from fastapi import APIRouter
from app.services.motor_v4 import analizar_estadisticas

router = APIRouter(prefix="/stats", tags=["Estadísticas"])

@router.get("/")
async def get_stats():
    data = await analizar_estadisticas()
    return data
