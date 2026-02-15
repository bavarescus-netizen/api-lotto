from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# PARCHE DE RUTA: Forzamos a Python a ver la raíz para encontrar 'bd.py'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from bd import get_db
    from app.services.motor_v4 import generar_prediccion
except ImportError as e:
    print(f"Error en prediccion.py: {e}")
    raise

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/generar")
async def api_generar(db: AsyncSession = Depends(get_db)):
    try:
        resultado = await generar_prediccion(db)
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
