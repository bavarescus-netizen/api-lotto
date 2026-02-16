from fastapi import APIRouter, Depends, HTTPException, Form
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db
from datetime import datetime
import pytz

router = APIRouter(prefix="/historico", tags=["Historial"])

@router.post("/agregar")
async def agregar_resultado(
    animalito: str = Form(...),
    hora: str = Form(...), # Formato "10:00 AM"
    fecha: str = Form(None),
    db: AsyncSession = Depends(get_db)
):
    """
    Registra el resultado oficial y calibra la IA automáticamente.
    """
    try:
        tz = pytz.timezone('America/Caracas')
        fecha_final = fecha if fecha else datetime.now(tz).strftime("%Y-%m-%d")

        # 1. INSERTAR EN EL HISTÓRICO (Los 28,709 + 1)
        query_hist = text("""
            INSERT INTO historico (fecha, hora, animalito)
            VALUES (:f, :h, :a)
        """)
        await db.execute(query_hist, {"f": fecha_final, "h": hora, "a": animalito.lower()})

        # 2. CALIBRACIÓN INSTANTÁNEA
        # Buscamos si hubo una predicción para esa misma fecha y hora para marcar ACIERTO
        query_audit = text("""
            UPDATE auditoria_ia 
            SET resultado_real = :a,
                acierto = (LOWER(TRIM(animal_predicho)) = LOWER(TRIM(:a)))
            WHERE fecha = :f AND hora = :h AND acierto IS NULL
        """)
        await db.execute(query_audit, {"f": fecha_final, "h": hora, "a": animalito.lower()})
        
        await db.commit()
        
        return {
            "status": "success", 
            "mensaje": f"Resultado {animalito} registrado y IA calibrada para las {hora}."
        }
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/ultimos")
async def obtener_ultimos(db: AsyncSession = Depends(get_db)):
    """Trae los últimos 10 resultados para mostrar en el Dashboard"""
    query = text("SELECT fecha, hora, animalito FROM historico ORDER BY fecha DESC, hora DESC LIMIT 10")
    res = await db.execute(query)
    return [{"fecha": r[0], "hora": r[1], "animal": r[2]} for r in res.fetchall()]
