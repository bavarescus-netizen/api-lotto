"""
PREDICCION.PY — Ruta de predicción
Actualizado V10 — usa motor_v10
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from db import get_db
from app.services.motor_v10 import generar_prediccion

router = APIRouter(tags=["Predicción"])

@router.get("/prediccion")
async def api_obtener_prediccion(db: AsyncSession = Depends(get_db)):
    try:
        resultado = await generar_prediccion(db)
        if not resultado or not resultado.get("top3"):
            raise HTTPException(status_code=404, detail="Sin predicción generada")
        return resultado
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
