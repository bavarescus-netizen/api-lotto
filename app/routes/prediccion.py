from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# Forzar detección de la raíz
sys.path.append(os.getcwd())

from db import get_db
from app.services.motor_v4 import generar_prediccion

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/generar")
async def api_generar(db: AsyncSession = Depends(get_db)):
    try:
        # El motor ahora se encarga de guardar en auditoria_ia automáticamente
        return await generar_prediccion(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
