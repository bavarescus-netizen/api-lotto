from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db
from app.services.motor_v4 import analizar_estadisticas

router = APIRouter(tags=["Estadísticas"])

@router.get("/stats")
async def api_obtener_stats(db: AsyncSession = Depends(get_db)):
    """
    Entrega métricas de verdad y datos históricos para el Dashboard.
    """
    try:
        # 1. Obtener frecuencia para el gráfico (Top 10)
        stats_data = await analizar_estadisticas(db)
        
        # 2. Calcular Efectividad Real desde auditoria_ia
        query_efectividad = text("""
            SELECT 
                COALESCE(
                    (COUNT(CASE WHEN acierto = TRUE THEN 1 END)::FLOAT / 
                    NULLIF(COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END), 0) * 100), 
                    0
                ) as porcentaje
            FROM auditoria_ia
        """)
        res = await db.execute(query_efectividad)
        efectividad_val = res.scalar() or 0.0

        return {
            "status": "success",
            "efectividad": f"{round(efectividad_val, 1)}%",
            "data": stats_data.get("data", {}),
            "total_analizado": 28709
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


