from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# === PARCHE DEFENSIVO DE RUTAS ===
# Esto obliga a Python a mirar la carpeta raíz donde está tu base de datos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    # Verificamos si tu archivo es 'bd' o 'db'
    if os.path.exists(os.path.join(BASE_DIR, "bd.py")):
        from bd import get_db
    else:
        from db import get_db
        
    from app.services.motor_v4 import generar_prediccion
except ImportError as e:
    print(f"Error importando módulos: {e}")
    raise

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/generar")
async def api_generar(db: AsyncSession = Depends(get_db)):
    try:
        return await generar_prediccion(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
