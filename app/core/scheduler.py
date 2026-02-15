import asyncio
from datetime import datetime
from app.services.scraper import obtener_ultimo_resultado
from app.services.guardar_sorteo import guardar_sorteo
from app.services.evaluar_prediccion import evaluar

# Variable global para evitar procesar lo mismo 60 veces por hora
ULTIMO_SORTEO_PROCESADO = None

async def ciclo_infinito():
    global ULTIMO_SORTEO_PROCESADO
    
    print("🚀 Sistema Vivo: Iniciando monitoreo de 11 sorteos diarios...")

    while True:
        try:
            # 1. Obtención de datos
            data = obtener_ultimo_resultado() # { 'numero': '15', 'hora': '10:00 AM' }

            if data and data != ULTIMO_SORTEO_PROCESADO:
                # 2. Persistencia en Neon (Solo si es nuevo)
                guardado = await guardar_sorteo(data)

                if guardado:
                    print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Sorteo detectado: {data}")
                    
                    # 3. El Sistema "Aprende": Evalúa si la predicción fue acertada
                    resultado_eval = await evaluar(data)
                    print(f"📈 Análisis de precisión: {resultado_eval}")
                    
                    ULTIMO_SORTEO_PROCESADO = data
            
            # Lógica de espera inteligente
            ahora = datetime.now()
            # Si estamos fuera del rango de sorteos (ej. 8 PM a 8 AM), dormimos más
            if ahora.hour < 9 or ahora.hour > 19:
                sleep_time = 600  # 10 minutos (ahorro de recursos en Render)
            else:
                sleep_time = 60   # 1 minuto durante horario de sorteos
                
        except Exception as e:
            print(f"⚠️ Error en ciclo: {e}")
            sleep_time = 120  # Si falla, esperamos 2 min para reintentar
            
        await asyncio.sleep(sleep_time)
