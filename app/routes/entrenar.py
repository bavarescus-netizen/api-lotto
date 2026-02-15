from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import os
import sys

# Forzar que el directorio raíz sea visible
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

try:
    import bd
    from bd import get_db
    from app.services.motor_v4 import entrenar_modelo_v4
except ImportError as e:
    # Si falla, intentamos importar como modulo de app
    try:
        from app.bd import get_db
    except:
        print(f"Error persistente en rutas: {e}")
        raise

router = APIRouter(prefix="/entrenar", tags=["Entrenamiento"])

@router.get("/procesar")
async def api_entrenar(db: AsyncSession = Depends(get_db)):
    try:
        return await entrenar_modelo_v4(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
