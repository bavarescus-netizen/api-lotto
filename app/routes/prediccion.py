dfrom fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# Esto le dice a Python que mire en /opt/render/project/src
sys.path.append(os.getcwd())

try:
    # Intentamos la importación directa desde la raíz
    import db
    from db import get_db
    from app.services.motor_v4 import entrenar_modelo_v4
except ImportError as e:
    print(f"DEBUG: No se encontró bd.py. Archivos en raíz: {os.listdir(os.getcwd())}")
    raise HTTPException(status_code=500, detail=f"Error de configuración: {str(e)}")

router = APIRouter(prefix="/entrenar", tags=["Entrenamiento"])

@router.get("/procesar")
async def api_entrenar(db: AsyncSession = Depends(get_db)):
    try:
        return await entrenar_modelo_v4(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
