"""
METRICAS.PY — Ruta de métricas
Actualizado V10 — usa motor_v10
"""
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from db import get_db
from app.services.motor_v10 import obtener_estadisticas

router = APIRouter()

@router.get("/metricas")
async def metricas(db: AsyncSession = Depends(get_db)):
    return await obtener_estadisticas(db)
