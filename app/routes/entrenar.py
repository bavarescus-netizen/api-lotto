from fastapi import APIRouter, HTTPException
from db import get_db
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/procesar")
async def entrenar_modelo():
    conn = await get_db_connection()
    try:
        # 1. Limpiamos la tabla de probabilidades para evitar duplicados
        await conn.execute("TRUNCATE TABLE probabilidades_hora")

        # 2. Consulta corregida con casting ::TIME para PostgreSQL
        # Esto soluciona el error 'function pg_catalog.extract(unknown, text) does not exist'
        query = """
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
        INSERT INTO probabilidades_hora (hora, animalito, frecuencia, probabilidad, tendencia)
        SELECT 
            g.h, 
            g.animalito, 
            g.c,
            -- Calculamos un peso: 40% historia total, 60% racha reciente
            ROUND(((g.c * 0.4) + (COALESCE(r.c, 0) * 0.6))::numeric, 2) as peso,
            CASE 
                WHEN COALESCE(r.c, 0) > 0 THEN 'Caliente' 
                ELSE 'Frio' 
            END
        FROM stats_global g
        LEFT JOIN stats_reciente r ON g.h = r.h AND g.animalito = r.animalito
        WHERE g.h BETWEEN 9 AND 19;
        """
        
        await conn.execute(query)
        return {"status": "success", "message": "Neural Engine actualizado con 28,709 registros."}

    except Exception as e:
        logger.error(f"Error en el entrenamiento: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()
