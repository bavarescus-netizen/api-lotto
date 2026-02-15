import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

# === CONFIGURACIÓN DE RUTAS ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

# Importamos las rutas con el nombre del paquete 'app'
try:
    from app.routes import prediccion, entrenar, historico, stats
except ImportError as e:
    print(f"Error crítico importando rutas: {e}")
    # Si falla, imprimimos dónde está buscando Python para debug
    print(f"Buscando en: {sys.path}")
    raise

app = FastAPI(title="Lotto AI V4")

# === ARCHIVOS ESTÁTICOS ===
# Según tu lista, la carpeta se llama 'imagenes'
imagenes_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(imagenes_path):
    app.mount("/static", StaticFiles(directory=imagenes_path), name="static")

# === ROUTERS ===
app.include_router(prediccion.router)
app.include_router(entrenar.router)
app.include_router(historico.router)
app.include_router(stats.router)

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    # Ajustado a tu ruta: app/routes/dashboard.html
    html_file = os.path.join(BASE_DIR, "app", "routes", "dashboard.html")
    try:
        with open(html_file, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Lotto AI V4 Online</h1><p>Error: dashboard.html no encontrado.</p>"

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
