import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# Parche de rutas para que Render encuentre todo
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

app = FastAPI(title="Lotto AI V4")

# 1. Ajuste de Imágenes (Busca en la carpeta imagenes/ de la raíz)
app.mount("/static", StaticFiles(directory="imagenes"), name="static")

# 2. CORRECCIÓN DE RUTA: Apuntamos a donde está tu dashboard.html realmente
# Según tu captura, está en app/routes/
templates = Jinja2Templates(directory="app/routes")

# Importación de routers
from app.routes import prediccion, entrenar, historico, stats

app.include_router(prediccion.router)
app.include_router(entrenar.router)
app.include_router(historico.router)
app.include_router(stats.router)

# 3. Carga del Dashboard PRO desde la nueva ubicación
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    # Ya no hay error, buscará en app/routes/dashboard.html
    return templates.TemplateResponse("dashboard.html", {"request": request})

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
