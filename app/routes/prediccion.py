from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# Forzar detección de la raíz
sys.path.append(os.getcwd())

try:
    from db import get_db
    from app.services.motor_v4 import generar_prediccion
except ImportError as e:
    print(f"❌ Error en prediccion.py: {e}")
    raise

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/generar")
async def api_generar(db: AsyncSession = Depends(get_db)):
    try:
        return await generar_prediccion(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
