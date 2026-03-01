from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from db import get_db
from app.services.motor_v5 import generar_prediccion

router = APIRouter(tags=["Predicción"])

@router.get("/prediccion")
async def api_obtener_prediccion(db: AsyncSession = Depends(get_db)):
    try:
        resultado = await generar_prediccion(db)
        if "error" in resultado:
            raise HTTPException(status_code=404, detail=resultado["error"])
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
