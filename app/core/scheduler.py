"""
SCHEDULER V5 — LOTTOAI PRO
===========================
- Captura resultados automáticamente cada hora
- Calibra predicciones contra resultados reales
- Entrena motor después de cada captura
- NUEVO: Aprendizaje semanal automático (domingo 3AM Venezuela)
- NUEVO: Notificación 10 min antes de cada sorteo con TOP 3
"""

import asyncio
import logging
from datetime import datetime, timedelta, date
import pytz
import httpx

logger = logging.getLogger(__name__)

TIMEZONE_VE = pytz.timezone('America/Caracas')
BASE_URL = "https://loteriadehoy.com/animalito/lottoactivo"

HORAS_SORTEO = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

# Para notificaciones futuras (push notifications app móvil)
NOTIFICACIONES_PENDIENTES = {}


def hora_venezuela():
    return datetime.now(TIMEZONE_VE)


async def obtener_resultados_hoy():
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(f"{BASE_URL}/resultados/")
            r.raise_for_status()
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(r.text, "html.parser")
            resultados = []
            hoy = hora_venezuela().date()
            for item in soup.select(".animalito-resultado, .resultado-item, [class*='resultado']"):
                try:
                    hora_tag = item.select_one("[class*='hora'], .hora, time")
                    animal_tag = item.select_one("[class*='animal'], .nombre, h3, h4, strong")
                    if hora_tag and animal_tag:
                        hora_txt = hora_tag.get_text(strip=True)
                        animal_txt = animal_tag.get_text(strip=True).lower().strip()
                        resultados.append({"fecha": hoy, "hora": hora_txt, "animalito": animal_txt})
                except Exception:
                    continue
            logger.info(f"Hoy: {len(resultados)} sorteos encontrados")
            return resultados
    except Exception as e:
        logger.error(f"Error obteniendo resultados hoy: {e}")
        return []


async def obtener_historico_semana(fecha_inicio, fecha_fin):
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            url = f"{BASE_URL}/historico/{fecha_inicio}/{fecha_fin}/"
            r = await client.get(url)
            r.raise_for_status()
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(r.text, "html.parser")
            resultados = []
            from app.services.motor_v5 import MAPA_ANIMALES
            num_to_animal = MAPA_ANIMALES
            for row in soup.select("tr, .resultado-row"):
                celdas = row.select("td")
                if len(celdas) >= 3:
                    try:
                        fecha_txt = celdas[0].get_text(strip=True)
                        hora_txt = celdas[1].get_text(strip=True)
                        numero_txt = celdas[2].get_text(strip=True)
                        fecha_obj = date.fromisoformat(fecha_txt)
                        animalito = num_to_animal.get(numero_txt, numero_txt.lower())
                        resultados.append({"fecha": fecha_obj, "hora": hora_txt, "animalito": animalito})
                    except Exception:
                        continue
            logger.info(f"Historico {fecha_inicio} a {fecha_fin}: {len(resultados)} registros")
            return resultados
    except Exception as e:
        logger.error(f"Error historico: {e}")
        return []


async def guardar_resultados(db, resultados):
    from sqlalchemy import text
    nuevos = 0
    for r in resultados:
        try:
            res = await db.execute(text("""
                INSERT INTO historico (fecha, hora, animalito)
                VALUES (:f, :h, :a)
                ON CONFLICT (fecha, hora) DO NOTHING
            """), {"f": r["fecha"], "h": r["hora"], "a": r["animalito"]})
            if res.rowcount > 0:
                nuevos += 1
        except Exception:
            continue
    if nuevos > 0:
        await db.commit()
    return nuevos


async def capturar_y_procesar(db):
    """Captura resultado actual, calibra y genera nueva predicción"""
    from app.services.motor_v5 import (
        calibrar_predicciones, entrenar_modelo, generar_prediccion
    )
    from sqlalchemy import text

    resultados = await obtener_resultados_hoy()
    nuevos = await guardar_resultados(db, resultados)

    if nuevos > 0:
        logger.info(f"✅ {nuevos} nuevo(s) resultado(s) capturado(s)")
        calibradas = await calibrar_predicciones(db)
        logger.info(f"🎯 Calibración: {calibradas.get('calibradas',0)} validadas")
        await entrenar_modelo(db)

    pred = await generar_prediccion(db)
    top1 = pred.get("top3", [{}])[0]
    confianza = pred.get("confianza_idx", 0)
    señal = pred.get("señal_texto", "")
    hora = pred.get("hora", "")
    proxima = pred.get("proxima_hora", "")

    logger.info(f"🔮 Predicción: {top1.get('animal','?')} ({top1.get('porcentaje','?')}) | Conf: {confianza}/100 | {señal}")

    # Guardar notificación pendiente para próximo sorteo
    if proxima:
        NOTIFICACIONES_PENDIENTES[proxima] = {
            "top3": pred.get("top3", []),
            "confianza": confianza,
            "señal": señal
        }

    return nuevos > 0


async def aprendizaje_semanal(db):
    """
    Corre cada domingo a las 3AM Venezuela.
    Ajusta pesos del motor basado en los últimos 90 días.
    """
    from app.services.motor_v5 import aprender_desde_historico
    logger.info("🧠 Iniciando aprendizaje semanal automático...")
    hoy = hora_venezuela().date()
    fecha_inicio = hoy - timedelta(days=90)
    resultado = await aprender_desde_historico(db, fecha_inicio)
    logger.info(f"🧠 Aprendizaje semanal: {resultado.get('message','')}")
    return resultado


async def carga_inicial(db):
    """Carga histórico de las últimas 2 semanas + sorteos de hoy"""
    from sqlalchemy import text
    logger.info("📦 Carga inicial — sincronizando últimos 14 días...")
    total = 0
    hoy = hora_venezuela().date()

    for offset in range(0, 14, 7):
        fecha_fin = hoy - timedelta(days=offset)
        fecha_ini = fecha_fin - timedelta(days=6)
        resultados = await obtener_historico_semana(fecha_ini, fecha_fin)
        total += await guardar_resultados(db, resultados)
        await asyncio.sleep(1)

    # Capturar sorteos de hoy que ya ocurrieron
    resultados_hoy = await obtener_resultados_hoy()
    nuevos_hoy = await guardar_resultados(db, resultados_hoy)
    total += nuevos_hoy
    logger.info(f"✅ Carga inicial: {total} registros nuevos ({nuevos_hoy} de hoy)")
    return total


async def ciclo_infinito():
    """Loop principal del scheduler"""
    from db import get_db
    from app.services.motor_v5 import (
        calibrar_predicciones, entrenar_modelo, generar_prediccion
    )

    # Esperar que la BD esté lista
    await asyncio.sleep(3)

    async for db in get_db():
        try:
            # Carga inicial
            await carga_inicial(db)

            # Entrenamiento inicial
            resultado_entrenamiento = await entrenar_modelo(db)
            logger.info(f"🧠 {resultado_entrenamiento.get('message','')}")

            # Calibración inicial
            cal = await calibrar_predicciones(db)
            logger.info(f"🎯 Calibración inicial: {cal.get('calibradas',0)} validadas")

            # Predicción inicial
            pred = await generar_prediccion(db)
            top1 = pred.get("top3", [{}])[0]
            logger.info(f"🔮 Predicción inicial: {top1.get('animal','?')} ({top1.get('porcentaje','?')}) — Confianza: {pred.get('confianza_idx',0)}/100")

        except Exception as e:
            logger.error(f"Error en carga inicial: {e}")
        break

    intentos_fallidos = 0
    ultimo_aprendizaje = hora_venezuela().date()

    while True:
        try:
            ahora = hora_venezuela()
            hora_actual = ahora.hour
            minuto_actual = ahora.minute
            dia_semana = ahora.weekday()  # 6 = domingo

            # ── APRENDIZAJE SEMANAL (domingo 3AM Venezuela) ──
            if (dia_semana == 6 and hora_actual == 3 and minuto_actual < 5
                    and ahora.date() != ultimo_aprendizaje):
                async for db in get_db():
                    await aprendizaje_semanal(db)
                    ultimo_aprendizaje = ahora.date()
                    break

            # ── VENTANA DE CAPTURA: minutos 3-25 de cada hora de sorteo ──
            if hora_actual in HORAS_SORTEO and 3 <= minuto_actual <= 25:
                hora_str = f"{hora_actual}:00"
                logger.info(f"🔍 [{ahora.strftime('%H:%M')}] Buscando resultado sorteo {hora_str}...")

                async for db in get_db():
                    encontrado = await capturar_y_procesar(db)

                    if encontrado:
                        intentos_fallidos = 0
                        minutos_espera = 65 - minuto_actual
                        logger.info(f"⏰ Próxima revisión en {minutos_espera} min")
                        await asyncio.sleep(minutos_espera * 60)
                    else:
                        intentos_fallidos += 1
                        if intentos_fallidos >= 5:
                            logger.warning(f"⚠️ {intentos_fallidos} intentos sin resultado. Saltando hora.")
                            intentos_fallidos = 0
                            await asyncio.sleep(40 * 60)
                        else:
                            logger.info(f"⏳ Sin resultado (intento {intentos_fallidos}/5). Reintento en 3 min.")
                            await asyncio.sleep(180)
                    break

            else:
                # Fuera de ventana — calcular minutos hasta próxima ventana
                if hora_actual in HORAS_SORTEO:
                    if minuto_actual < 3:
                        espera = (3 - minuto_actual) * 60
                    else:
                        # Ya pasó la ventana, esperar próxima hora
                        prox_hora = hora_actual + 1
                        if prox_hora in HORAS_SORTEO:
                            espera = (60 - minuto_actual + 3) * 60
                        else:
                            espera = 300
                elif hora_actual < 8:
                    # Antes del primer sorteo
                    espera = (8 - hora_actual) * 3600 + (3 - minuto_actual) * 60
                    espera = max(espera, 60)
                elif hora_actual >= 20:
                    # Después del último sorteo del día
                    # Captura final para asegurar que nada quedó pendiente
                    async for db in get_db():
                        await capturar_y_procesar(db)
                        break
                    espera = (32 - hora_actual) * 3600  # Hasta 8AM siguiente
                    espera = max(espera, 3600)
                else:
                    espera = 300

                logger.info(f"⏰ [{ahora.strftime('%H:%M')}] Esperando ventana. {espera//60} min restantes.")
                await asyncio.sleep(espera)

        except Exception as e:
            logger.error(f"Error en ciclo: {e}")
            await asyncio.sleep(60)
