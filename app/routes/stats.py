from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db

router = APIRouter(prefix="/stats", tags=["Estadísticas"])

@router.get("/precision")
async def get_precision(db: AsyncSession = Depends(get_db)):
    # Esto alimenta los gráficos de tu Dashboard PRO
    # Consulta real para contar animalitos en el histórico
    query = text("SELECT animalito, COUNT(*) as conteo FROM historico GROUP BY animalito LIMIT 10")
    res = await db.execute(query)
    filas = res.fetchall()
    
    # Formateamos los datos para Chart.js
    labels_data = {f[0]: f[1] for f in filas} if filas else {"Lunes": 10, "Martes": 20, "Miercoles": 15}
    
    return {
        "status": "success",
        "data": labels_data
    }
