from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# Forzar detección de db.py en la raíz
sys.path.append(os.getcwd())

try:
    from db import get_db
    from app.services.motor_v4 import entrenar_modelo_v4
    # Importamos el calibrador que le da memoria a la IA
    from app.services.calibrador import calibrar_resultados_ia 
except ImportError as e:
    print(f"❌ Error en entrenar.py: {e}")
    raise

router = APIRouter(prefix="/entrenar", tags=["Entrenamiento"])

@router.get("/procesar")
async def api_entrenar(db: AsyncSession = Depends(get_db)):
    try:
        # PASO 1: Calibrar (Mirar qué predijo vs qué salió en el historial)
        # Esto llena la columna 'acierto' en tu tabla auditoria_ia
        resultado_calibracion = await calibrar_resultados_ia(db)
        
        # PASO 2: Entrenar (Sincronizar patrones 2018-2026 con los aciertos nuevos)
        resultado_motor = await entrenar_modelo_v4(db)
        
        return {
            "status": "success",
            "calibracion": resultado_calibracion,
            "entrenamiento": resultado_motor,
            "analisis": "Memoria de patrones actualizada con éxito."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
