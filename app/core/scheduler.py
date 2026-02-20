import asyncio
from datetime import datetime
# Ajustamos las importaciones para que coincidan con tu estructura de carpetas
from services.scraper import obtener_ultimo_resultado
from services.guardar_sorteo import guardar_sorteo
from services.evaluar_prediccion import evaluar

ULTIMO_SORTEO_PROCESADO = None

async def ciclo_infinito():
    global ULTIMO_SORTEO_PROCESADO
    
    print("🚀 Sistema Vivo: Iniciando monitoreo de 11 sorteos diarios...")

    while True:
        try:
            # 1. Obtención de datos (Llamada al Scraper)
            data = await obtener_ultimo_resultado() # IMPORTANTE: Debe ser async si usas httpx o aiohttp

            if data and data != ULTIMO_SORTEO_PROCESADO:
                # 2. Persistencia en Neon (Solo si es nuevo)
                # Aquí se mete el resultado al 'historico'
                guardado = await guardar_sorteo(data)

                if guardado:
                    print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Sorteo detectado: {data}")
                    
                    # 3. El Sistema "Aprende": Evalúa el acierto/fallo en auditoria_ia
                    resultado_eval = await evaluar(data)
                    print(f"📈 Análisis de precisión: {resultado_eval}")
                    
                    ULTIMO_SORTEO_PROCESADO = data
            
            # Lógica de espera inteligente
            ahora = datetime.now()
            # De 9 AM a 7 PM (19) vigilamos cada minuto
            if 9 <= ahora.hour <= 19:
                sleep_time = 60  
            else:
                sleep_time = 600 # Fuera de horario, descansamos 10 min
                
        except Exception as e:
            print(f"⚠️ Error en ciclo: {e}")
            sleep_time = 120 
            
        await asyncio.sleep(sleep_time)
