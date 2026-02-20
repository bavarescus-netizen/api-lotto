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

# 1. Ajuste de rutas para que Render encuentre tus carpetas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "app"))

app = FastAPI(title="Lotto AI V4.5 PRO")

# 2. Configuración de Archivos Estáticos y HTML
# Imágenes en la raíz / Templates en app/routes/
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# 3. Importaciones siguiendo tu estructura de carpetas
from db import get_db
# Servicios y Scraper
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance, examen_cerebro
from app.services.scraper import descargar_rango_historico
# Automatización
from app.core.scheduler import ciclo_infinito 

# Routers de la carpeta app/routes/
from app.routes import prediccion, entrenar, stats, historico

app.include_router(prediccion.router, prefix="/api", tags=["IA"])
app.include_router(entrenar.router, prefix="/api", tags=["Motor"])
app.include_router(stats.router, prefix="/api", tags=["Stats"])
app.include_router(historico.router, prefix="/api", tags=["Historial"])

# --- RUTA PARA ACTIVAR EL SCRAPER (EL BOTÓN) ---
@app.get("/api/examen-real")
async def ejecutar_examen(db: AsyncSession = Depends(get_db)):
    """Sincroniza manualmente los datos de loteriadehoy.com a tu base Neon"""
    try:
        # Iniciamos carga desde el último punto registrado (7 Feb) hasta hoy
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
                """), {
                    "f": reg["fecha"], 
                    "h": reg["hora"], 
                    "a": reg["animalito"], 
                    "l": reg["loteria"]
                })
                if result.rowcount > 0:
                    agregados += 1
            await db.commit()
        
        reporte = await examen_cerebro(db)
        return {
            "status": "Éxito",
            "nuevos_registros": agregados,
            "mensaje": "Base de datos Neon actualizada correctamente",
            "resultado_ia": reporte
        }
    except Exception as e:
        await db.rollback()
        return {"status": "Error", "detalle": str(e)}

# 4. Ruta Home (Dashboard)
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    res_ia = await generar_prediccion(db)
    bitacora = await obtener_bitacora_avance(db)

    # Renderiza dashboard.html ubicado en app/routes/
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "version": "v4.5 PRO",
        "registros": "28,709+",
        "top3": res_ia.get("top3", []),
        "decision": res_ia.get("decision", "MOTOR ACTIVO"),
        "bitacora": bitacora,
        "url_sync": "/api/examen-real"
    })

# 5. Inicio del Bot Automático
@app.on_event("startup")
async def startup_event():
    # Activa el scheduler cada vez que el servidor Render despierta
    asyncio.create_task(ciclo_infinito())

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
