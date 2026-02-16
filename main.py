import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# Parche de rutas absoluto para evitar errores en Render
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

app = FastAPI(title="Lotto AI V4.5 PRO")

# 1. ARCHIVOS ESTÁTICOS (Imágenes de los animalitos)
# Asegúrate de que la carpeta se llame 'imagenes' y esté en la raíz
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "imagenes")), name="static")

# 2. TEMPLATES (Carga de dashboard.html)
# Buscamos en 'app/routes' que es donde tienes el HTML según tu estructura
template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# 3. IMPORTACIÓN DE ROUTERS
from app.routes import prediccion, entrenar, historico, stats

# 4. CONEXIÓN DE RUTAS CON PREFIJOS (Vital para que el JS funcione)
# El Dashboard busca '/api/prediccion', así que añadimos el prefix '/api'
app.include_router(prediccion.router, prefix="/api") 

# El Dashboard busca '/entrenar/procesar'
app.include_router(entrenar.router) 

# El Dashboard busca '/api/stats'
app.include_router(stats.router, prefix="/api")

app.include_router(historico.router)

# 5. RUTA PRINCIPAL (DASHBOARD)
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """
    Renderiza el Dashboard Principal.
    Asegúrate de que 'dashboard.html' esté dentro de 'app/routes/'
    """
    return templates.TemplateResponse("dashboard.html", {"request": request})

# 6. EJECUCIÓN
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
