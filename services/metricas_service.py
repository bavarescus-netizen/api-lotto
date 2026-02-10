from sqlalchemy import text

async def obtener_metricas(db):

    result = await db.execute(text("""
        SELECT
            COUNT(*) as total,
            COALESCE(SUM(acierto),0) as aciertos
        FROM metricas
    """))

    row = result.first()

    total = row.total or 0
    aciertos = row.aciertos or 0

    porcentaje = round((aciertos / total) * 100, 2) if total else 0

    return {
        "total_registros": total,
        "aciertos": aciertos,
        "fallos": total - aciertos,
        "precision": porcentaje
    }
