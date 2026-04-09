from fastapi import APIRouter, Depends, HTTPException, Form
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db
from datetime import datetime
from zoneinfo import ZoneInfo

router = APIRouter(prefix="/historico", tags=["Historial"])


@router.post("/agregar")
async def agregar_resultado(
    animalito: str = Form(...),
    hora: str = Form(...),
    fecha: str = Form(None),
    db: AsyncSession = Depends(get_db)
):
    try:
        tz = ZoneInfo('America/Caracas')
        fecha_final = fecha if fecha else datetime.now(tz).strftime("%Y-%m-%d")

        await db.execute(text("""
            INSERT INTO historico (fecha, hora, animalito)
            VALUES (:f, :h, :a)
        """), {"f": fecha_final, "h": hora, "a": animalito.lower()})

        await db.execute(text("""
            UPDATE auditoria_ia 
            SET resultado_real = :a,
                acierto = (LOWER(TRIM(animal_predicho)) = LOWER(TRIM(:a)))
            WHERE fecha = :f AND hora = :h AND acierto IS NULL
        """), {"f": fecha_final, "h": hora, "a": animalito.lower()})
        
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
    res = await db.execute(text(
        "SELECT fecha, hora, animalito FROM historico ORDER BY fecha DESC, hora DESC LIMIT 10"
    ))
    return [{"fecha": r[0], "hora": r[1], "animal": r[2]} for r in res.fetchall()]
