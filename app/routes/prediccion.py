from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# 1. PARCHE DE RUTA: Subimos dos niveles para encontrar 'bd.py' en la raíz
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# 2. IMPORTS CORREGIDOS:
try:
    # Importamos desde la raíz
    from bd import get_db
    # Importamos desde la carpeta services (donde está tu motor_v4.py)
    from app.services.motor_v4 import generar_prediccion
except ImportError as e:
    print(f"Error de rutas en prediccion.py: {e}")
    raise

router = APIRouter(prefix="/prediccion", tags=["Predicciones"])

@router.get("/generar")
async def api_generar(db: AsyncSession = Depends(get_db)):
    """
    Endpoint que conecta el Dashboard con el Motor V4
    """
    try:
        # Llamamos a la función del motor que está en app/services/motor_v4.py
        resultado = await generar_prediccion(db)
        
        if "error" in resultado:
            raise HTTPException(status_code=500, detail=resultado["error"])
            
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en la API: {str(e)}")
