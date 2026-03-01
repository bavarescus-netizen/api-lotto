"""
METRICAS_SERVICE.PY — Corregido
Usa la tabla 'metricas' (con 'a') que es la que existe en Neon.
"""

from sqlalchemy import text

async def obtener_metricas(db):
    try:
        result = await db.execute(text("""
            SELECT total, aciertos, errores, precision
            FROM metricas
            WHERE id = 1
        """))
        row = result.first()

        if not row:
            # Si no hay fila, calcular directo desde auditoria_ia
            result2 = await db.execute(text("""
                SELECT 
                    COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END) as total,
                    COUNT(CASE WHEN acierto = TRUE THEN 1 END) as aciertos,
                    COUNT(CASE WHEN acierto = FALSE THEN 1 END) as errores,
                    ROUND(
                        (COUNT(CASE WHEN acierto = TRUE THEN 1 END)::FLOAT / 
                        NULLIF(COUNT(CASE WHEN acierto IS NOT NULL THEN 1 END), 0)) * 100
                    , 1) as precision
                FROM auditoria_ia
            """))
            row = result2.first()

        if not row:
            return {"total_registros": 0, "aciertos": 0, "fallos": 0, "precision": 0}

        total = int(row[0] or 0)
        aciertos = int(row[1] or 0)
        errores = int(row[2] or 0)
        precision = float(row[3] or 0)

        return {
            "total_registros": total,
            "aciertos": aciertos,
            "fallos": errores,
            "precision": round(precision, 1)
        }
    except Exception as e:
        return {"total_registros": 0, "aciertos": 0, "fallos": 0, "precision": 0, "error": str(e)}
