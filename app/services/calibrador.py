from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

async def calibrar_resultados_ia(db: AsyncSession):
    """
    Compara las predicciones pendientes con los resultados reales 
    e identifica patrones de éxito o fallo.
    """
    try:
        # 1. Buscar predicciones que aún no han sido validadas (acierto es NULL)
        # Cruzamos con la tabla historico por fecha y hora
        query_validador = text("""
            UPDATE auditoria_ia a
            SET acierto = (a.animal_predicho = h.animalito),
                resultado_real = h.animalito
            FROM historico h
            WHERE a.fecha = h.fecha 
              AND a.hora = h.hora
              AND a.acierto IS NULL;
        """)
        
        result = await db.execute(query_validador)
        await db.commit()
        
        filas_validadas = result.rowcount
        
        # 2. Análisis de "Números Fríos" (Opcional: para saber qué faltó)
        # Esto ayuda a la IA a ver qué animales están saliendo que ella no predijo
        return {
            "status": "success",
            "mensaje": f"Se calibraron {filas_validadas} resultados correctamente.",
            "total_procesado": filas_validadas
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "mensaje": str(e)}
