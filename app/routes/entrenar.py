from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# Forzar detección de db.py en la raíz
sys.path.append(os.getcwd())

try:
    from db import get_db
    from app.services.motor_v4 import entrenar_modelo_v4
except ImportError as e:
    print(f"❌ Error en entrenar.py: {e}")
    raise

router = APIRouter(prefix="/entrenar", tags=["Entrenamiento"])

@router.get("/procesar")
async def api_entrenar(db: AsyncSession = Depends(get_db)):
    try:
        return await entrenar_modelo_v4(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
