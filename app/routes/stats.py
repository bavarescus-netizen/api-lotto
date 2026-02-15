from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

sys.path.append(os.getcwd())

try:
    from db import get_db
    from app.services.motor_v4 import analizar_estadisticas
except ImportError as e:
    print(f"❌ Error en stats.py: {e}")
    raise

router = APIRouter(prefix="/stats", tags=["Estadísticas"])

@router.get("/analisis")
async def api_stats(db: AsyncSession = Depends(get_db)):
    try:
        return await analizar_estadisticas(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
