from sqlalchemy import text

async def obtener_metricas(db):

    result = await db.execute(text("""
        SELECT
            COUNT(*) as total,
            COALESCE(SUM(aciertos),0) as aciertos
        FROM metricas
    """))

    row = result.first()

    if not row:
        return {
            "total_registros": 0,
            "aciertos": 0,
            "fallos": 0,
            "precision": 0
        }

    total = int(row.total or 0)
    aciertos = int(row.aciertos or 0)

    return {
        "total_registros": total,
        "aciertos": aciertos,
        "fallos": total - aciertos,
        "precision": round((aciertos / total) * 100, 2) if total else 0
    }
