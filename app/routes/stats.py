from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db
from app.services.motor_v4 import analizar_estadisticas

router = APIRouter(prefix="/stats", tags=["Stats"])

@router.get("/")
async def get_stats(db: AsyncSession = Depends(get_db)):
    # A. Datos para el gráfico
    chart_data = await analizar_estadisticas(db)
    
    # B. Calcular Efectividad Global
    query_ef = text("""
        SELECT (COUNT(CASE WHEN acierto = TRUE THEN 1 END)::FLOAT / 
        NULLIF(COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END), 0) * 100)
        FROM auditoria_ia
    """)
    res_ef = await db.execute(query_ef)
    efectividad = res_ef.scalar() or 0
    
    return {
        "status": "success",
        "data": chart_data.get("data", {}),
        "efectividad": f"{round(efectividad, 1)}%"
    }
