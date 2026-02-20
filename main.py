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

# 1. Ajuste de rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# 2. Archivos estáticos y Templates
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# 3. Importaciones
from db import get_db
from services.motor_v4 import generar_prediccion, obtener_bitacora_avance, examen_cerebro
from services.scraper import descargar_rango_historico
from core.scheduler import ciclo_infinito  # <-- El corazón
from routes import prediccion, entrenar, stats, historico

app.include_router(prediccion.router, prefix="/api", tags=["IA"])
app.include_router(entrenar.router, prefix="/api", tags=["Motor"])
app.include_router(stats.router, prefix="/api", tags=["Stats"])
app.include_router(historico.router, prefix="/api", tags=["Historial"])

# --- NUEVA RUTA: EL EXAMEN AL CEREBRO ---
@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    """Descarga datos de febrero y califica la IA"""
    try:
        inicio = datetime(2026, 2, 7)
        fin = datetime.now()
        
        print(f"📅 Iniciando examen: {inicio.date()} al {fin.date()}")
        datos_nuevos = await descargar_rango_historico(inicio, fin)
        
        if datos_nuevos:
            for reg in datos_nuevos:
                await db.execute(text("""
                    INSERT INTO historico (fecha, hora, animalito, loteria)
                    VALUES (:f, :h, :a, :l)
                    ON CONFLICT (fecha, hora, loteria) DO NOTHING
                """), {"f": reg["fecha"], "h": reg["hora"], "a": reg["animalito"], "l": reg["loteria"]})
            await db.commit()
        
        reporte = await examen_cerebro(db)
        return {
            "status": "Examen completado",
            "registros_procesados": len(datos_nuevos),
            "resultado": reporte
        }
    except Exception as e:
        return {"error": str(e)}

# 4. Ruta de inicio
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
        "bitacora": bitacora
    })

# --- EVENTO DE ARRANQUE: ACTIVA EL MONITOR ---
@app.on_event("startup")
async def startup_event():
    # Iniciamos el ciclo infinito en segundo plano
    asyncio.create_task(ciclo_infinito())

# 5. Ejecución
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
