from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from bd import get_db # Asegúrate de que la ruta sea correcta

router = APIRouter(prefix="/historico", tags=["Historial"])

@router.get("/")
async def obtener_historial(db: AsyncSession = Depends(get_db)):
    try:
        # Consulta los últimos 10 sorteos
        query = text("SELECT fecha, hora, animalito, loteria FROM historico ORDER BY fecha DESC, hora DESC LIMIT 10")
        result = await db.execute(query)
        
        # Transformamos a la lista que el Dashboard espera
        data = []
        for fila in result:
            data.append({
                "fecha": str(fila.fecha),
                "hora": fila.hora,
                "animal": fila.animalito.upper(),
                "loteria": fila.loteria
            })
        return data
    except Exception as e:
        return {"error": str(e)}
