from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
# Importamos las rutas
from app.routes import prediccion, entrenar, historico, stats 

# ESTA LÍNEA ES LA QUE RENDER BUSCA (Debe estar al nivel 0 de indentación)
app = FastAPI(title="Lotto AI V4")

# Montar archivos estáticos
try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except:
    print("Aviso: Carpeta static no encontrada, continuando sin ella.")

# Incluir las rutas
app.include_router(prediccion.router)
app.include_router(entrenar.router)
app.include_router(historico.router)
app.include_router(stats.router)

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    try:
        # Verifica que la ruta al HTML sea correcta según tu estructura en GitHub
        with open("app/routes/dashboard.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Lotto AI V4 Activa</h1><p>Error: dashboard.html no encontrado.</p>"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
