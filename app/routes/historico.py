from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from bd import get_db

router = APIRouter(prefix="/historico", tags=["Historial"])

@router.get("/")
async def get_historico(db: AsyncSession = Depends(get_db)):
    query = text("""
        SELECT fecha, hora, animalito, loteria 
        FROM historico 
        ORDER BY fecha DESC, hora DESC 
        LIMIT 15
    """)
    result = await db.execute(query)
    
    lista = []
    for fila in result:
        lista.append({
            "fecha": str(fila.fecha),
            "hora": fila.hora,
            "animal": fila.animalito,
            "loteria": fila.loteria
        })
    return lista
