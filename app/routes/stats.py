from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# === PARCHE DEFENSIVO DE RUTAS ===
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    if os.path.exists(os.path.join(BASE_DIR, "bd.py")):
        from bd import get_db
    else:
        from db import get_db
        
    # Importamos tu función de estadísticas
    from app.services.motor_v4 import analizar_estadisticas
except ImportError as e:
    print(f"Error importando en stats.py: {e}")
    raise

router = APIRouter(prefix="/stats", tags=["Estadísticas"])

@router.get("/analisis")
async def api_stats(db: AsyncSession = Depends(get_db)):
    try:
        return await analizar_estadisticas(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
