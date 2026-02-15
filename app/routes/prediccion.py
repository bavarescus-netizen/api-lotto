from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# Parche de emergencia para encontrar 'bd.py' en la raíz
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from bd import get_db
except ImportError:
    # Intento alternativo si el path falla
    from ...bd import get_db

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/")
async def obtener_prediccion(db: AsyncSession = Depends(get_db)):
    try:
        # Aquí irá tu lógica de predicción llamando al motor
        return {"status": "ok", "message": "Motor V4 listo para predecir"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
