import sys
import os
import asyncio
import re
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime, date

# 1. Configuración de Rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# 2. Estáticos
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app", "routes"))

from db import get_db
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance
from app.services.scraper import descargar_rango_historico
from app.core.scheduler import ciclo_infinito 

# Routers
from app.routes import prediccion, entrenar, stats, historico
app.include_router(prediccion.router, prefix="/api")

# --- RUTA DE SINCRONIZACIÓN CORREGIDA ---
@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    try:
        hoy_puro = date.today()
        inicio_puro = date(2026, 2, 7)
        datos_nuevos = await descargar_rango_historico(inicio_puro, hoy_puro)
        
        agregados = 0
        if datos_nuevos:
            for reg in datos_nuevos:
                f_raw = reg["fecha"]
                
                # Manejo de fecha ultra-seguro
                if isinstance(f_raw, str):
                    f_val = datetime.strptime(f_raw, '%Y-%m-%d').date()
                elif hasattr(f_raw, "date") and callable(getattr(f_raw, "date")):
                    f_val = f_raw.date()
                else:
                    f_val = f_raw # Ya es date

                if f_val > hoy_puro: continue

                # A. Histórico
                await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l)
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {"f": f_val, "h": reg["hora"], "a": reg["animalito"], "l": reg["loteria"]})
                
                # B. Auditoría (Ahora sí funcionará el ON CONFLICT con la PK de arriba)
                await db.execute(text("""
                    UPDATE auditoria_ia 
                    SET resultado_real = :a,
                        acierto = (LOWER(animal_predicho) = LOWER(REGEXP_REPLACE(:a, '[^a-zA-ZáéíóúñÁÉÍÓÚÑ]', '', 'g')))
                    WHERE fecha = :f AND hora = :h 
                """), {"a": reg["animalito"], "f": f_val, "h": reg["hora"]})

            await db.commit()
            agregados = len(datos_nuevos)
        
        return JSONResponse({"status": "success", "message": f"Sincronizado OK. {agregados} items."})
    except Exception as e:
        await db.rollback()
        print(f"❌ Error Sincro: {str(e)}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# --- HOME ---
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    res_ia = {"top3": [], "analisis": "IA Activa"}
    ultimos_12 = []
    
    try:
        res_ia = await generar_prediccion(db)
        # Historial sin columna ID
        res_db = await db.execute(text("SELECT hora, animalito FROM historico ORDER BY fecha DESC, hora DESC LIMIT 12"))
        for r in res_db.fetchall():
            img = re.sub(r'[^a-záéíóúñ]', '', r[1].lower()).strip() + ".png"
            ultimos_12.append({"hora": r[0], "animal": r[1].upper(), "img": img})
        
        bitacora = await obtener_bitacora_avance(db)
        await db.commit()
    except Exception as e:
        print(f"Error Home: {e}")
        bitacora = []
        await db.rollback()

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "top3": res_ia.get("top3", []),
        "bitacora": bitacora,
        "ultimos_db": ultimos_12,
        "analisis": res_ia.get("analisis", "Esperando datos...")
    })

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(ciclo_infinito())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
