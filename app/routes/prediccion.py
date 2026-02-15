from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# Parche para que este archivo "vea" la raíz
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Imports absolutos
from bd import get_db
from app.services.motor_v4 import generar_prediccion

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/generar")
async def api_generar(db: AsyncSession = Depends(get_db)):
    try:
        resultado = await generar_prediccion(db)
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
