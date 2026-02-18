from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.db import get_db

router = APIRouter()

@router.get("/procesar")
async def procesar_entrenamiento(db: AsyncSession = Depends(get_db)):
    try:
        # 1. Limpiar predicciones previas
        await db.execute(text("TRUNCATE TABLE probabilidades_hora"))
        
        # 2. SQL con casting explícito ::TIME para evitar el error 500
        query = text("""
            INSERT INTO probabilidades_hora (hora, animalito, frecuencia, probabilidad, tendencia)
            WITH stats_global AS (
                SELECT 
                    EXTRACT(HOUR FROM hora::TIME)::INT as h, 
                    animalito, 
                    COUNT(*) as c
                FROM historico 
                GROUP BY 1, 2
            ),
            stats_reciente AS (
                SELECT 
                    EXTRACT(HOUR FROM hora::TIME)::INT as h, 
                    animalito, 
                    COUNT(*) as c
                FROM historico 
                WHERE fecha >= CURRENT_DATE - INTERVAL '15 days' 
                GROUP BY 1, 2
            )
            SELECT 
                g.h, 
                g.animalito, 
                g.c,
                ((g.c * 0.4) + (COALESCE(r.c, 0) * 0.6)) as peso,
                CASE WHEN COALESCE(r.c, 0) > 0 THEN 'Caliente' ELSE 'Frío' END
            FROM stats_global g
            LEFT JOIN stats_reciente r ON g.h = r.h AND g.animalito = r.animalito
            WHERE g.h BETWEEN 9 AND 19
        """)
        
        await db.execute(query)
        await db.commit()
        return {"status": "success", "message": "Motor entrenado con 28k registros"}
    except Exception as e:
        await db.rollback()
        return {"status": "error", "detail": str(e)}
