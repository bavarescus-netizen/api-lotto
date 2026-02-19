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

# 2. CONFIGURACIÓN DE RUTAS DE ARCHIVOS (CORREGIDO)
# Las imágenes están en la raíz, pero el HTML espera /imagenes/
imagenes_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(imagenes_path):
    app.mount("/imagenes", StaticFiles(directory=imagenes_path), name="imagenes")

# Los templates suelen estar en app/templates, no en routes
template_path = os.path.join(BASE_DIR, "app", "templates") # Cambiado a templates
templates = Jinja2Templates(directory=template_path)

# 3. Importación de servicios y base de datos
from app.db import get_db
from app.services.motor_v4 import generar_prediccion, obtener_bitacora_avance 
from app.routes import prediccion, entrenar, stats, historico

# Registro de Routers
app.include_router(prediccion.router, prefix="/api", tags=["IA"])
app.include_router(entrenar.router, prefix="/api", tags=["Motor"])
app.include_router(stats.router, prefix="/api", tags=["Stats"])
app.include_router(historico.router, prefix="/api", tags=["Historial"])

# 4. RUTA DE INICIO CON DATOS REALES (CORREGIDO)
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: AsyncSession = Depends(get_db)):
    try:
        # LLAMAMOS AL CEREBRO: Obtenemos predicción y bitácora real
        datos_ia = await generar_prediccion(db)
        avance_bot = await obtener_bitacora_avance(db)
        
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "version": "v4.5 PRO",
            "registros": "28,709",
            "top3": datos_ia.get("top3", []),
            "decision": datos_ia.get("decision", "MOTOR ACTIVO"),
            "bitacora": avance_bot # Aquí inyectamos la lista de aciertos/fallos
        })
    except Exception as e:
        print(f"Error en Dashboard: {e}")
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "version": "ERROR",
            "registros": "0",
            "top3": [],
            "decision": "ERROR DE CONEXIÓN CON NEON DB",
            "bitacora": []
        })

# 5. Ejecución
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=True)
