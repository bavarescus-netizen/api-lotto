import sys
import os
import asyncio
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime

# 1. Ajuste de rutas (Base para Render)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# 2. Archivos estáticos y Templates
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

template_path = os.path.join(BASE_DIR, "app", "templates") # Asegúrate que esta sea tu carpeta
templates = Jinja2Templates(directory=template_path)

# 3. Importaciones de tus servicios
from db import get_db
from services.motor_v4 import generar_prediccion, obtener_bitacora_avance, examen_cerebro
from services.scraper import descargar_rango_historico
from core.scheduler import ciclo_infinito 

# Importación de Routers
from routes import prediccion, entrenar, stats, historico
app.include_router(prediccion.router, prefix="/api", tags=["IA"])
app.include_router(entrenar.router, prefix="/api", tags=["Motor"])
app.include_router(stats.router, prefix="/api", tags=["Stats"])
app.include_router(historico.router, prefix="/api", tags=["Historial"])

# --- NUEVA RUTA: ACTIVACIÓN POR BOTÓN (EXAMEN REAL) ---
@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    """Esta es la función que activa el scraper desde el navegador"""
    try:
        # Iniciamos desde el último registro conocido en tu Neon (7 de Feb)
        inicio = datetime(2026, 2, 7)
        fin = datetime.now()
        
        print(f"📡 Iniciando sincronización manual: {inicio.date()} al {fin.date()}")
        datos_nuevos = await descargar_rango_historico(inicio, fin)
        
        agregados = 0
        if datos_nuevos:
            for reg in datos_nuevos:
                # Insertamos evitando duplicados
                result = await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l)
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {"f": reg["fecha"], "h": reg["hora"], "a": reg["animalito"], "l": reg["loteria"]})
                if result.rowcount > 0:
                    agregados += 1
            
            await db.commit()
        
        # Después de cargar, calificamos a la IA
        reporte = await examen_cerebro(db)
        
        return {
            "status": "Sincronización Exitosa",
            "nuevos_registros": agregados,
            "total_leidos": len(datos_nuevos),
            "evaluacion_ia": reporte
        }
    except Exception as e:
        await db.rollback()
        return {"status": "Error", "detalle": str(e)}

# 4. Ruta Home (Dashboard)
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    res_ia = await generar_prediccion(db)
    bitacora = await obtener_bitacora_avance(db)

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "version": "v4.5 PRO",
        "registros": "28,709+",
        "top3": res_ia.get("top3", []),
        "decision": res_ia.get("decision", "MOTOR ACTIVO"),
        "bitacora": bitacora,
        "url_sync": "/api/examen-real" # Variable para el botón
    })

# 5. Evento de Arranque (Automatización)
@app.on_event("startup")
async def startup_event():
    # Esto activa el bot que busca resultados cada 30 min solo
    asyncio.create_task(ciclo_infinito())

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
