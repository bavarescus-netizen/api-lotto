from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# Forzar detección de la raíz
sys.path.append(os.getcwd())

from db import get_db
from app.services.motor_v4 import generar_prediccion

# El prefijo ya es /prediccion, que combinado con /api en main.py da /api/prediccion
router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/") # <--- CAMBIO AQUÍ: Quitamos "/generar" para que coincida con el fetch('/api/prediccion')
async def api_generar(db: AsyncSession = Depends(get_db)):
    try:
        # El motor V4 ya devuelve el JSON con top3, animal, imagen y porcentaje
        return await generar_prediccion(db)
    except Exception as e:
        print(f"❌ Error en Motor V4: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
