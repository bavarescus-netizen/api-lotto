from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# === PARCHE DE RUTA PARA RENDER (VERIFICADO) ===
# Subimos dos niveles para llegar a la raíz donde está 'bd.py'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    # Ahora sí encontrará 'bd' porque la raíz está en sys.path
    from bd import get_db
    # Importamos desde services usando la ruta del paquete
    from app.services.motor_v4 import generar_prediccion
except ImportError as e:
    print(f"Error de importación en prediccion.py: {e}")
    raise

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/generar")
async def api_generar(db: AsyncSession = Depends(get_db)):
    try:
        resultado = await generar_prediccion(db)
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
