import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

# === 1. CONFIGURACIÓN ABSOLUTA DE RUTAS ===
# Esto asegura que Python encuentre 'bd.py', 'app/', y 'imagenes/' 
# sin importar desde dónde se ejecute el script en el servidor.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# === 2. IMPORTACIÓN DE RUTAS (CON MANEJO DE ERRORES) ===
try:
    from app.routes import prediccion, entrenar, historico, stats
except ImportError as e:
    print(f"❌ ERROR CRÍTICO: No se pudo importar un módulo. {e}")
    print(f"🔍 INFO DE RUTAS: BASE_DIR es {BASE_DIR}")
    print(f"🔍 PATHS ACTUALES: {sys.path}")
    raise

app = FastAPI(title="Lotto AI V4")

# === 3. ARCHIVOS ESTÁTICOS (Carpeta 'imagenes') ===
# Mapeamos la carpeta física 'imagenes' al prefijo URL '/static'
imagenes_path = os.path.join(BASE_DIR, "imagenes")
if os.path.exists(imagenes_path):
    app.mount("/static", StaticFiles(directory=imagenes_path), name="static")
    print(f"✅ Carpeta de imágenes montada en: {imagenes_path}")
else:
    print(f"⚠️ AVISO: No se encontró la carpeta 'imagenes' en {imagenes_path}")

# === 4. INCLUSIÓN DE ROUTERS ===
app.include_router(prediccion.router)
app.include_router(entrenar.router)
app.include_router(historico.router)
app.include_router(stats.router)

# === 5. DASHBOARD PRINCIPAL ===
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    # Ruta exacta a tu archivo HTML según tu estructura
    html_file = os.path.join(BASE_DIR, "app", "routes", "dashboard.html")
    try:
        with open(html_file, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return f"""
        <html>
            <body style="font-family: sans-serif; text-align: center; padding-top: 50px;">
                <h1 style="color: #2c3e50;">🚀 Lotto AI V4 Online</h1>
                <p style="color: red;">Error: dashboard.html no encontrado.</p>
                <p>Ubicación buscada: <code>{html_file}</code></p>
            </body>
        </html>
        """

# === 6. EJECUCIÓN (Render usa el puerto dinámico) ===
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
