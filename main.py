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

app = FastAPI(title="Lotto AI V4")

# 1. Imágenes: Buscamos en la raíz/imagenes
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "imagenes")), name="static")

# 2. Templates: Buscamos donde está el HTML (app/routes)
# Usamos path.join para que Render no se pierda
template_path = os.path.join(BASE_DIR, "app", "routes")
templates = Jinja2Templates(directory=template_path)

# Importación de routers
from app.routes import prediccion, entrenar, historico, stats

app.include_router(prediccion.router)
app.include_router(entrenar.router)
app.include_router(historico.router)
app.include_router(stats.router)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    # Esto cargará tu dashboard.html sin errores de ruta
    return templates.TemplateResponse("dashboard.html", {"request": request})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
