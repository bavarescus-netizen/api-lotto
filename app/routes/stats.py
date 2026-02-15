from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db

router = APIRouter(prefix="/stats", tags=["Estadísticas"])

@router.get("/precision")
async def get_precision(db: AsyncSession = Depends(get_db)):
    """
    Alimenta los gráficos del Dashboard con la efectividad real 
    extraída de la tabla de auditoría.
    """
    try:
        # 1. Gráfico de Aciertos por Horario (Línea de tiempo)
        # Filtramos los últimos 7 días para ver la evolución
        query_linea = text("""
            SELECT hora, 
                   COUNT(*) FILTER (WHERE acierto = TRUE) * 100.0 / NULLIF(COUNT(*), 0) as efectividad
            FROM auditoria_ia
            WHERE acierto IS NOT NULL
            GROUP BY hora
            ORDER BY MIN(timestamp_registro) ASC
        """)
        res_linea = await db.execute(query_linea)
        filas_linea = res_linea.fetchall()

        # 2. Gráfico de Frecuencia de Animalitos Ganadores (Barras)
        query_barras = text("""
            SELECT resultado_real, COUNT(*) as conteo
            FROM auditoria_ia
            WHERE acierto = TRUE
            GROUP BY resultado_real
            ORDER BY conteo DESC
            LIMIT 7
        """)
        res_barras = await db.execute(query_barras)
        filas_barras = res_barras.fetchall()

        # Formateo para Chart.js
        # Si no hay datos, enviamos un ejemplo para que el gráfico no se vea vacío
        data_efectividad = {f[0]: round(float(f[1]), 2) for f in filas_linea} if filas_linea else {"9am": 0, "12pm": 0, "4pm": 0}
        data_animales = {f[0].capitalize(): f[1] for f in filas_barras} if filas_barras else {"Sin Datos": 0}

        return {
            "status": "success",
            "grafico_linea": data_efectividad,
            "grafico_barras": data_animales,
            "resumen": {
                "total_predicciones": len(filas_linea),
                "meta_efectividad": "45%"
            }
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}
