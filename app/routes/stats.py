"""
STATS.PY — Panel de estadísticas FUNCIONAL
Reemplaza la versión que decía "en mantenimiento"
"""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from db import get_db
from app.services.motor_v5 import obtener_estadisticas, obtener_bitacora

router = APIRouter()

@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    stats = await obtener_estadisticas(db)
    bitacora = await obtener_bitacora(db)

    return JSONResponse({
        "status": "success",
        "stats": stats,
        "bitacora_hoy": bitacora,
        "message": (
            f"Efectividad: {stats['efectividad_global']}% | "
            f"Hoy: {stats['aciertos_hoy']}/{stats['sorteos_hoy']} aciertos | "
            f"{stats['total_historico']:,} registros históricos"
        )
    })
