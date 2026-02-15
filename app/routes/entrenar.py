from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# Parche de rutas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from bd import get_db
from motor_v4 import entrenar_modelo_v4

router = APIRouter(prefix="/entrenar", tags=["Entrenamiento"])

@router.get("/procesar")
async def api_entrenar(db: AsyncSession = Depends(get_db)):
    return await entrenar_modelo_v4(db)
