from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db
from app.services.motor_v4 import entrenar_modelo_v4

router = APIRouter(prefix="/entrenar", tags=["Entrenamiento"])

@router.get("/procesar")
async def api_entrenar(db: AsyncSession = Depends(get_db)):
    try:
        # 1. Borrar memoria vieja
        await db.execute(text("TRUNCATE TABLE probabilidades_hora"))

        # 2. APRENDIZAJE: Analizar 28,709 registros (60% peso a lo reciente)
        query_aprendizaje = text("""
            INSERT INTO probabilidades_hora (hora, animalito, frecuencia, probabilidad, tendencia)
            WITH stats_global AS (
                SELECT EXTRACT(HOUR FROM hora)::INT as h, animalito, COUNT(*) as c
                FROM historico GROUP BY 1, 2
            ),
            stats_reciente AS (
                SELECT EXTRACT(HOUR FROM hora)::INT as h, animalito, COUNT(*) as c
                FROM historico WHERE fecha >= CURRENT_DATE - INTERVAL '15 days' GROUP BY 1, 2
            )
            SELECT g.h, g.animalito, g.c,
                   ((g.c * 0.4) + (COALESCE(r.c, 0) * 0.6)) as peso,
                   CASE WHEN COALESCE(r.c, 0) > 0 THEN 'Caliente' ELSE 'Frío' END
            FROM stats_global g
            LEFT JOIN stats_reciente r ON g.h = r.h AND g.animalito = r.animalito
            WHERE g.h BETWEEN 9 AND 19
        """)
        await db.execute(query_aprendizaje)
        
        # 3. Calibrar aciertos
        aciertos_sinc = await entrenar_modelo_v4(db)
        
        await db.commit()
        return {
            "status": "success",
            "mensaje": f"Cerebro entrenado con 28,709 registros. {aciertos_sinc} aciertos calibrados."
        }
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
