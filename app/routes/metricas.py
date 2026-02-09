from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from db import get_db

router = APIRouter(prefix="/metricas", tags=["metricas"])


@router.get("/")
async def metricas(db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN acierto = true THEN 1 ELSE 0 END) AS aciertos
        FROM predicciones
    """))

    row = result.fetchone()

    total = row.total or 0
    aciertos = row.aciertos or 0
    precision = (aciertos / total * 100) if total else 0

    return {
        "total_predicciones": total,
        "aciertos": aciertos,
        "fallos": total - aciertos,
        "precision_%": round(precision, 2)
    }
