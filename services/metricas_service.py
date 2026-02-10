from sqlalchemy import text

async def obtener_metricas(db):

    result = await db.execute(text("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN acierto=1 THEN 1 ELSE 0 END) as aciertos
        FROM metricas
    """))

    row = result.first()

    if not row or row.total == 0:
        return {
            "total": 0,
            "aciertos": 0,
            "porcentaje": 0
        }

    porcentaje = round((row.aciertos / row.total) * 100, 2)

    return {
        "total": row.total,
        "aciertos": row.aciertos,
        "porcentaje": porcentaje
    }
