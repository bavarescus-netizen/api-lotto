import asyncio
from app.services.scraper import obtener_ultimo_resultado
from app.services.guardar_sorteo import guardar_sorteo
from app.services.evaluar_prediccion import evaluar


async def ciclo_infinito():

    while True:

        try:
            data = obtener_ultimo_resultado()

            if data:
                guardado = await guardar_sorteo(data)

                if guardado:
                    print("🟢 Nuevo sorteo:", data)

                    resultado = await evaluar(data)
                    print("📊 Evaluación:", resultado)

        except Exception as e:
            print("ERROR:", e)

        await asyncio.sleep(60)  # cada minuto
