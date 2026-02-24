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

# 1. Rutas de sistema
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# 2. Archivos Estáticos e Imágenes
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# 3. Importaciones de servicios
from db import get_db
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance
from app.services.scraper import descargar_rango_historico
from app.core.scheduler import ciclo_infinito 

# Routers
from app.routes import prediccion, entrenar, stats, historico
app.include_router(prediccion.router, prefix="/api", tags=["IA"])
app.include_router(entrenar.router, prefix="/api", tags=["Motor"])
app.include_router(stats.router, prefix="/api", tags=["Stats"])
app.include_router(historico.router, prefix="/api", tags=["Historial"])

# --- RUTA DE SINCRONIZACIÓN (LA SOLUCIÓN AL ERROR 500) ---
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
                
                # LA CORRECCIÓN: Manejo inteligente de tipos de fecha
                if isinstance(f_raw, str):
                    f_val = datetime.strptime(f_raw, '%Y-%m-%d').date()
                elif isinstance(f_raw, datetime):
                    f_val = f_raw.date()
                else:
                    f_val = f_raw # Ya es un objeto date puro
                
                if f_val > hoy_puro: continue

                # A. Insertar en Histórico
                await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l)
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {"f": f_val, "h": reg["hora"], "a": reg["animalito"], "l": reg["loteria"]})
                
                # B. Actualizar Auditoría
                await db.execute(text("""
                    UPDATE auditoria_ia 
                    SET resultado_real = :a,
                        acierto = (LOWER(animal_predicho) = LOWER(REGEXP_REPLACE(:a, '[^a-zA-ZáéíóúñÁÉÍÓÚÑ]', '', 'g')))
                    WHERE fecha = :f AND hora = :h 
                    AND (resultado_real = 'PENDIENTE' OR resultado_real IS NULL)
                """), {"a": reg["animalito"], "f": f_val, "h": reg["hora"]})

            await db.commit()
            agregados = len(datos_nuevos)
        
        return JSONResponse({"status": "success", "message": f"Sincronizado OK. {agregados} procesados."})
    except Exception as e:
        await db.rollback()
        print(f"❌ Error Crítico Sincro: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# --- RUTA DASHBOARD (CORREGIDA SIN ID) ---
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    res_ia = {"top3": [], "analisis": "Motor V4.5 PRO ONLINE"}
    bitacora = []
    ultimos_12 = []

    try:
        # 1. Predicción
        try:
            res_ia = await generar_prediccion(db)
            await db.commit()
        except: await db.rollback()

        # 2. Historial
        res_db = await db.execute(text("""
            SELECT hora, animalito FROM historico 
            ORDER BY fecha DESC, hora DESC LIMIT 12
        """))
        for r in res_db.fetchall():
            nombre_img = re.sub(r'[^a-záéíóúñ]', '', r[1].lower()).strip()
            ultimos_12.append({
                "hora": r[0], "animal": r[1].upper(), "img": f"{nombre_img}.png"
            })
        await db.commit()

        # 3. Bitácora
        try:
            bitacora = await obtener_bitacora_avance(db)
            await db.commit()
        except: await db.rollback()

    except Exception as e:
        print(f"⚠️ Error Home: {e}")
        await db.rollback()

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "top3": res_ia.get("top3", []),
        "bitacora": bitacora,
        "ultimos_db": ultimos_12,
        "analisis": res_ia.get("analisis", "Análisis IA completado")
    })

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(ciclo_infinito())

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
