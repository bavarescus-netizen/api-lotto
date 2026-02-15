from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
import sys
import os

# Aseguramos que el sistema encuentre db.py y los servicios
sys.path.append(os.getcwd())

try:
    from db import get_db
    # Ahora entrenar_modelo_v4 ya incluye la lógica de calibración cruzada
    from app.services.motor_v4 import entrenar_modelo_v4
except ImportError as e:
    print(f"❌ Error crítico en entrenar.py: {e}")
    raise

router = APIRouter(prefix="/entrenar", tags=["Entrenamiento"])

@router.get("/procesar")
async def api_entrenar(db: AsyncSession = Depends(get_db)):
    """
    Endpoint de Re-Calibración:
    Sincroniza lo que la IA predijo con los resultados reales del histórico
    para generar métricas de efectividad reales.
    """
    try:
        # Ejecutamos el motor de entrenamiento/calibración mejorado
        # Este proceso ahora hace el UPDATE masivo en auditoria_ia
        resultado = await entrenar_modelo_v4(db)
        
        if resultado.get("status") == "error":
            raise HTTPException(status_code=500, detail=resultado.get("mensaje"))
        
        return {
            "status": "success",
            "mensaje": resultado.get("mensaje"),
            "logs": resultado.get("logs"),
            "timestamp": "Sync: 2018-2026",
            "analisis": "Cerebro cuántico calibrado con nuevos aciertos."
        }
        
    except Exception as e:
        print(f"❌ Fallo en el proceso de entrenamiento: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Fallo sistémico: {str(e)}")
