import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

# PARCHE DE RUTAS: Asegura que el servidor vea db.py y la carpeta app
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

app = FastAPI(title="Lotto AI V4")

# Importación segura de rutas
try:
    from app.routes import prediccion, entrenar, historico, stats
except ImportError as e:
    print(f"❌ Error crítico importando rutas: {e}")
    raise

# Montar carpeta de imágenes
imagenes_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(imagenes_path):
    app.mount("/static", StaticFiles(directory=imagenes_path), name="static")

# Registro de Routers
app.include_router(prediccion.router)
app.include_router(entrenar.router)
app.include_router(historico.router)
app.include_router(stats.router)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    html_path = os.path.join(BASE_DIR, "app", "routes", "dashboard.html")
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    except:
        return "<h1>API Lotto AI v4 Online</h1>"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
