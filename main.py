from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from app.routes import prediccion, entrenar, historico, stats  # Asegúrate que estos archivos existan

app = FastAPI(title="Lotto AI V4")

# Montar archivos estáticos (imágenes, etc.)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Incluir las rutas
app.include_router(prediccion.router)
app.include_router(entrenar.router)
app.include_router(historico.router)
app.include_router(stats.router)

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    # Intentamos abrir el archivo HTML que diseñamos
    try:
        with open("app/routes/dashboard.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Error: dashboard.html no encontrado en app/routes/</h1>"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
