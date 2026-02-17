import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# Parche de rutas absoluto para Render
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

app = FastAPI(title="Lotto AI V4.5 PRO")

# 1. ARCHIVOS ESTÁTICOS (Para logos y fotos de animalitos)
static_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(static_path):
    app.mount("/static", StaticFiles(directory=static_path), name="static")

# 2. TEMPLATES (Donde vive tu dashboard.html)
template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# 3. IMPORTACIÓN DE ROUTERS
from app.routes import prediccion, entrenar, historico, stats

# 4. CONEXIÓN DE RUTAS (Prefijos sincronizados con el Dashboard)
app.include_router(prediccion.router, prefix="/api", tags=["Predicción"])
app.include_router(stats.router, prefix="/api", tags=["Estadísticas"])
app.include_router(entrenar.router, prefix="/api", tags=["Entrenamiento"]) # Ahora es /api/entrenar/procesar
app.include_router(historico.router, prefix="/api", tags=["Histórico"])

# 5. RUTA PRINCIPAL (Dashboard)
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """
    Carga el Centro de Control Neural Engine V4.5 PRO.
    """
    try:
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "version": "v4.5 PRO",
            "sync": "2018-2026",
            "registros": "28,709"
        })
    except Exception as e:
        return HTMLResponse(content=f"Error cargando dashboard: {str(e)}", status_code=500)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
