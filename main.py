import sys
import os
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

# 1. Configuración de rutas del sistema
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

app = FastAPI(title="Lotto AI V4.5 PRO")

# 2. Archivos estáticos y Templates (MANTENIENDO TU RUTA)
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    # IMPORTANTE: El HTML busca "/imagenes/", así que montamos la carpeta raíz ahí
    app.mount("/imagenes", StaticFiles(directory=static_path), name="imagenes")

# MANTENEMOS TU RUTA DE TEMPLATES QUE NO QUIERES CAMBIAR
template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# 3. Importación y Registro de Routers
from app.db import get_db
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance
from app.routes import prediccion, entrenar, stats, historico

app.include_router(prediccion.router, prefix="/api", tags=["IA"])
app.include_router(entrenar.router, prefix="/api", tags=["Motor"])
app.include_router(stats.router, prefix="/api", tags=["Stats"])
app.include_router(historico.router, prefix="/api", tags=["Historial"])

# 4. Ruta de inicio CORREGIDA para que envíe los Animalitos
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    # 1. Obtenemos la predicción real de la base de datos
    resultado_ia = await generar_prediccion(db)
    
    # 2. Obtenemos los últimos 5 resultados (Aciertos/Fallos)
    avance_bot = await obtener_bitacora_avance(db)

    # 3. Enviamos TODO al dashboard.html
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "version": "v4.5 PRO",
        "registros": "28,709",
        "top3": resultado_ia.get("top3", []),
        "decision": resultado_ia.get("decision", "LISTO PARA OPERAR"),
        "bitacora": avance_bot  # Esto es lo que quita el "solo texto"
    })

# 5. Ejecución
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
