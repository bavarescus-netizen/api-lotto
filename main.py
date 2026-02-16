import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# Parche de rutas absoluto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

app = FastAPI(title="Lotto AI V4.5 PRO")

# 1. ARCHIVOS ESTÁTICOS
# Asegura que la carpeta 'imagenes' esté en la raíz del proyecto
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "imagenes")), name="static")

# 2. TEMPLATES
# Dashboard debe estar en: app/routes/dashboard.html
template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# 3. IMPORTACIÓN DE ROUTERS
from app.routes import prediccion, entrenar, historico, stats

# 4. CONEXIÓN DE RUTAS (Prefijos unificados para el JS)
app.include_router(prediccion.router, prefix="/api")  # /api/prediccion
app.include_router(stats.router, prefix="/api")       # /api/stats
app.include_router(entrenar.router)                   # /entrenar/procesar
app.include_router(historico.router)

# 5. RUTA PRINCIPAL
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """
    Renderiza el Dashboard Principal inyectando el estado operativo.
    """
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "version": "v4.5 PRO",
        "sync": "2018-2026"
    })

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
