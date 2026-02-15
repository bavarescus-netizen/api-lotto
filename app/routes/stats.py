from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from bd import get_db

router = APIRouter(prefix="/stats", tags=["Estadísticas"])

@router.get("/precision")
async def get_precision(db: AsyncSession = Depends(get_db)):
    # Lógica para calcular efectividad por día
    query = text("""
        SELECT 
            EXTRACT(DOW FROM fecha) as dia_semana,
            COUNT(*) as total_sorteos
        FROM historico 
        WHERE fecha >= '2019-01-01'
        GROUP BY dia_semana
    """)
    result = await db.execute(query)
    
    # Mapeo de días (0=Domingo, 1=Lunes...)
    dias = ["Domingo", "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado"]
    stats = {dias[int(row.dia_semana)]: row.total_sorteos for row in result}
    
    return {
        "resumen": "Días con mayor volumen de datos analizados",
        "data": stats,
        "mejor_dia": max(stats, key=stats.get) if stats else "N/A"
    }
