"""
STATS.PY — Panel de estadísticas
Actualizado V10 — usa motor_v10
"""
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from db import get_db
from app.services.motor_v10 import obtener_estadisticas, obtener_bitacora

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
            f"Efectividad: {stats.get('efectividad_global',0)}% | "
            f"Hoy: {stats.get('aciertos_hoy',0)}/{stats.get('sorteos_hoy',0)} aciertos | "
            f"{stats.get('total_historico',0):,} registros históricos"
        )
    })
