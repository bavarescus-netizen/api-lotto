from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db

router = APIRouter(tags=["Stats"])

@router.get("/")
async def get_stats(db: AsyncSession = Depends(get_db)):
    try:
        # Consulta para la gráfica de barras (Top 10 más frecuentes)
        query_frecuencia = text("""
            SELECT animalito, COUNT(*) as total 
            FROM historico 
            GROUP BY animalito 
            ORDER BY total DESC LIMIT 10
        """)
        result = await db.execute(query_frecuencia)
        rows = result.fetchall()
        
        # Formato para el Dashboard: {"Perro": 50, "Gato": 30...}
        data_grafica = {row[0].upper(): row[1] for row in rows}
        
        # Consulta para los mini-logs de aciertos
        query_logs = text("""
            SELECT fecha, hora, animal_pronosticado, acierto 
            FROM auditoria_ia 
            ORDER BY id DESC LIMIT 5
        """)
        res_logs = await db.execute(query_logs)
        logs_recientes = [
            {"hora": str(r[1]), "animal": r[2], "acierto": r[3]} 
            for r in res_logs.fetchall()
        ]

        return {
            "status": "success",
            "data": data_grafica,
            "logs_recientes": logs_recientes
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}
