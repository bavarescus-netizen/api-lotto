from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db
from app.services.motor_v4 import generar_prediccion

router = APIRouter(tags=["Predicción"])

@router.get("/prediccion")
async def api_obtener_prediccion(db: AsyncSession = Depends(get_db)):
    """
    Consulta la predicción basada en el análisis de 28,709 registros.
    Busca cumplir la meta de 5 aciertos diarios.
    """
    try:
        resultado = await generar_prediccion(db)
        
        if "error" in resultado:
            raise HTTPException(status_code=404, detail=resultado["error"])
            
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en motor neural: {str(e)}")
