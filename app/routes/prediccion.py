from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# Aseguramos que el archivo sepa dónde está la raíz
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# IMPORTANTE: Importamos 'bd' directamente desde la raíz
from bd import get_db
# Importamos el motor desde la carpeta services
from app.services.motor_v4 import generar_prediccion

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/generar")
async def api_generar(db: AsyncSession = Depends(get_db)):
    try:
        resultado = await generar_prediccion(db)
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
