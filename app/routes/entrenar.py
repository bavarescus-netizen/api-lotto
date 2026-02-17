from fastapi import APIRouter, HTTPException
# Importación exacta según tu raíz: app/db.py -> get_db
from app.db import get_db 
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/procesar")
async def entrenar_modelo():
    # Establecemos conexión con tu lógica de Neon
    conn = await get_db()
    try:
        # 1. Limpiamos la tabla de resultados calculados
        await conn.execute("TRUNCATE TABLE probabilidades_hora")

        # 2. Inyección de lógica de predicción corregida
        # Se añade ::TIME y ::DATE para que PostgreSQL procese los strings sin error 500
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
            WHERE fecha::DATE >= CURRENT_DATE - INTERVAL '15 days' 
            GROUP BY 1, 2
        )
        INSERT INTO probabilidades_hora (hora, animalito, frecuencia, probabilidad, tendencia)
        SELECT 
            g.h, 
            g.animalito, 
            g.c,
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
        return {"status": "success", "message": "Motor V4.5 PRO sincronizado con éxito."}

    except Exception as e:
        logger.error(f"Error crítico en el motor: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Cerramos conexión para no saturar Neon
        await conn.close()
