import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

# === PARCHE DE RUTAS PARA RENDER ===
# Esto asegura que 'bd.py' sea visible para todos los módulos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# Importamos las rutas (asegúrate de que app/routes/ tenga un archivo __init__.py)
try:
    from app.routes import prediccion, entrenar, historico, stats
except ImportError as e:
    print(f"Error crítico importando rutas: {e}")
    raise

app = FastAPI(title="Lotto AI V4")

# === ARCHIVOS ESTÁTICOS ===
# Usamos ruta absoluta para evitar errores en el servidor
static_path = os.path.join(BASE_DIR, "static")
if os.path.exists(static_path):
    app.mount("/static", StaticFiles(directory=static_path), name="static")
else:
    print(f"Aviso: Carpeta {static_path} no encontrada.")

# === INCLUSIÓN DE ROUTERS ===
app.include_router(prediccion.router)
app.include_router(entrenar.router)
app.include_router(historico.router)
app.include_router(stats.router)

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    # Buscamos el HTML en la ruta que especificaste
    html_file = os.path.join(BASE_DIR, "app", "routes", "dashboard.html")
    
    try:
        with open(html_file, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return f"""
        <html>
            <body style="font-family: sans-serif; text-align: center; padding-top: 50px;">
                <h1>🚀 Lotto AI V4 Online</h1>
                <p style="color: red;">Error: No se encontró dashboard.html en {html_file}</p>
                <p>Las APIs están funcionando. Revisa la ubicación de tu archivo HTML.</p>
            </body>
        </html>
        """

if __name__ == "__main__":
    import uvicorn
    # Render asigna un puerto dinámico, por eso usamos la variable de entorno si existe
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
