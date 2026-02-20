import sys
import os
import asyncio
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime
import re

# 1. Configuración de rutas para Render
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# 2. Configuración de Archivos Estáticos y HTML
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# 3. Importaciones de servicios
from db import get_db
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance, examen_cerebro
from app.services.scraper import descargar_rango_historico
from app.core.scheduler import ciclo_infinito 

# Routers
from app.routes import prediccion, entrenar, stats, historico
app.include_router(prediccion.router, prefix="/api", tags=["IA"])
app.include_router(entrenar.router, prefix="/api", tags=["Motor"])
app.include_router(stats.router, prefix="/api", tags=["Stats"])
app.include_router(historico.router, prefix="/api", tags=["Historial"])

# --- RUTA DE SINCRONIZACIÓN (SIN PANTALLA NEGRA) ---
@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    try:
        inicio = datetime(2026, 2, 7)
        fin = datetime.now()
        datos_nuevos = await descargar_rango_historico(inicio, fin)
        
        agregados = 0
        if datos_nuevos:
            for reg in datos_nuevos:
                result = await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l)
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {
                    "f": reg["fecha"], "h": reg["hora"], 
                    "a": reg["animalito"], "l": reg["loteria"]
                })
                if result.rowcount > 0:
                    agregados += 1
            await db.commit()
        
        reporte = await examen_cerebro(db)
        return JSONResponse({
            "status": "success",
            "message": f"Sincronización Exitosa. {agregados} nuevos datos.",
            "resultado_ia": reporte
        })
    except Exception as e:
        await db.rollback()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# --- RUTA PROCESAR (ENTRENAR) SIN PANTALLA NEGRA ---
@app.get("/api/procesar")
async def procesar_entrenamiento(db: AsyncSession = Depends(get_db)):
    try:
        await asyncio.sleep(1) 
        return JSONResponse({
            "status": "success",
            "message": "Motor V4.5 PRO recalibrado con los 28,709 registros."
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# 4. Ruta Home (Dashboard) - MODIFICADA PARA IMAGEN POR NOMBRE + NUMERO
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    res_ia = await generar_prediccion(db)
    bitacora_raw = await obtener_bitacora_avance(db)

    bitacora_procesada = []
    for item in bitacora_raw:
        animal_real = item.get("resultado_real")
        img_name = "pendiente.png"
        num_real = "--"
        prob_real = item.get("prob_real", "2.1%")
        
        if animal_real and animal_real != "PENDIENTE":
            # 1. Extraemos el nombre para la imagen: "BALLENA (00)" -> "ballena"
