from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db

router = APIRouter()

@router.get("/metricas")
async def metricas(db: AsyncSession = Depends(get_db)):

    res = await db.execute(text("""
        SELECT
            COUNT(*) total,
            COUNT(*) FILTER (WHERE acertado = true) aciertos,
            COUNT(*) FILTER (WHERE acertado = false) errores
        FROM predicciones
    """))

    total, aciertos, errores = res.fetchone()

    precision = 0
    if total > 0:
        precision = round(aciertos * 100 / total, 2)

    return {
        "total": total,
        "aciertos": aciertos,
        "errores": errores,
        "precision": f"{precision}%"
    }
