from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# --- PARCHE DE EMERGENCIA PARA RENDER ---
# Esto obliga al archivo a mirar en la raíz para encontrar 'bd.py'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from bd import get_db
    from motor_v4 import generar_prediccion
except ImportError as e:
    print(f"Error de importación en prediccion.py: {e}")
    # Intento de respaldo relativo
    from ...bd import get_db
    from ...motor_v4 import generar_prediccion

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/generar")
async def api_generar(db: AsyncSession = Depends(get_db)):
    return await generar_prediccion(db)
