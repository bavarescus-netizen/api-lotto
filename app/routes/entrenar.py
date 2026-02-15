from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# PARCHE DE RUTA
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from bd import get_db
    from app.services.motor_v4 import entrenar_modelo_v4
except ImportError as e:
    print(f"Error en entrenar.py: {e}")
    raise

router = APIRouter(prefix="/entrenar", tags=["Entrenamiento"])

@router.get("/procesar")
async def api_entrenar(db: AsyncSession = Depends(get_db)):
    try:
        return await entrenar_modelo_v4(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
