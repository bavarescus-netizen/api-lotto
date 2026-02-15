import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

# === PARCHE DEFINITIVO DE RUTAS ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

# Importamos las rutas usando la ruta completa del paquete
try:
    from app.routes import prediccion, entrenar, historico, stats
except ImportError as e:
    print(f"Error crítico importando rutas: {e}")
    raise

app = FastAPI(title="Lotto AI V4")

# === ARCHIVOS ESTÁTICOS (Apuntando a tu carpeta 'imagenes') ===
if os.path.exists(os.path.join(BASE_DIR, "imagenes")):
    app.mount("/static", StaticFiles(directory="imagenes"), name="static")

# === REGISTRO DE RUTAS ===
app.include_router(prediccion.router)
app.include_router(entrenar.router)
app.include_router(historico.router)
app.include_router(stats.router)

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    html_file = os.path.join(BASE_DIR, "app", "routes", "dashboard.html")
    try:
        with open(html_file, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Lotto AI V4 Online</h1><p>Dashboard no encontrado.</p>"

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
