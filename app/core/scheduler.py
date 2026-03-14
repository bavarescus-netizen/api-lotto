import os
"""
SCHEDULER V6 — LOTTOAI PRO
===========================
FIXES vs V5:
  1. ❌ motor_v5 → ✅ motor_v9 (o v10 si existe)
  2. ❌ Selectores CSS inexistentes → ✅ scraper con regex robusto
  3. ❌ resultado_real nunca actualizado → ✅ UPDATE auditoria_ia post-captura
  4. ❌ INSERT con columna "hora" errónea → ✅ detecta hora_sorteo vs hora dinámicamente
  5. NUEVO: actualizar_auditoria_pendiente() — repara todas las filas PEND. existentes
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta, date
import pytz
import httpx

logger = logging.getLogger(__name__)

TIMEZONE_VE = pytz.timezone('America/Caracas')
BASE_URL     = "https://loteriadehoy.com/animalito/lottoactivo"
HORAS_SORTEO = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

# ── Mapa número → nombre de animal (índice Lotto Activo) ──
NUM_A_ANIMAL = {
    "1":"carnero",  "2":"toro",      "3":"ciempies", "4":"alacran",
    "5":"pavo",     "6":"cabra",     "7":"burro",    "8":"elefante",
    "9":"camello",  "10":"lechon",   "11":"yegua",   "12":"gallo",
    "13":"mono",    "14":"paloma",   "15":"oso",     "16":"lechuza",
    "17":"pavo",    "18":"gato",     "19":"caballo", "20":"perro",
    "21":"loro",    "22":"pato",     "23":"aguila",  "24":"rana",
    "25":"cebra",   "26":"iguana",   "27":"gallina", "28":"lapa",
    "29":"leon",    "30":"jirafa",   "31":"tortuga", "32":"delfin",
    "33":"perico",  "34":"ballena",  "35":"caiman",  "36":"tigre",
    "37":"venado",  "38":"ardilla",
}

# Regex que cubre todos los animales con variantes de tildes
_ANIMALES_RE = re.compile(
    r'\b(carnero|toro|ciempi[eé]s|alacr[aá]n|pavo(?:\s+real)?|cabra|burro|'
    r'elefante|camello|lech[oó]n|yegua|gallo|mono|paloma|oso|lechuza|gato|'
    r'caballo|perro|loro|pato|[aá]guila|rana|cebra|iguana|gallina|lapa|'
    r'le[oó]n|jirafa|tortuga|delf[ií]n|perico|ballena|caim[aá]n|tigre|'
    r'venado|ardilla)\b',
    re.IGNORECASE,
)

_HORA_RE = re.compile(r'\b(\d{1,2}:\d{2})\s*([AP]M)\b', re.IGNORECASE)


def hora_venezuela():
    return datetime.now(TIMEZONE_VE)


def _fmt_hora(h: str, ampm: str) -> str:
    """'8:00' + 'AM' → '08:00 AM'"""
    try:
        t = datetime.strptime(f"{h} {ampm.upper()}", "%I:%M %p")
        return t.strftime("%I:%M %p")          # → '08:00 AM'
    except Exception:
        return f"{h} {ampm.upper()}"


def _normalizar(nombre: str) -> str:
    n = nombre.lower().strip()
    # colapsar variantes
    reemplazos = {
        "alacrán": "alacran", "alacran": "alacran",
        "ciempiés": "ciempies",
        "lechón": "lechon",
        "pavo real": "pavo",
        "águila": "aguila",
        "caimán": "caiman",
        "delfín": "delfin",
        "león": "leon",
    }
    return reemplazos.get(n, n)


# ═══════════════════════════════════════════════════════════════
# SCRAPER — loteriadehoy.com  (3 estrategias en cascada)
# ═══════════════════════════════════════════════════════════════

async def obtener_resultados_hoy():
    """
    Estrategia 1: bloques con texto que contenga hora + animal en la misma línea/div
    Estrategia 2: filas <tr> con número de animal
    Estrategia 3: texto bruto de toda la página con regex
    """
    try:
        async with httpx.AsyncClient(
            timeout=20,
            headers={"User-Agent":
                     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/122.0 Safari/537.36"},
            follow_redirects=True,
        ) as client:
            r = await client.get(f"{BASE_URL}/resultados/")
            r.raise_for_status()
    except Exception as e:
        logger.error(f"[Scraper] HTTP error: {e}")
        return []

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(r.text, "html.parser")
    hoy = hora_venezuela().date()
    visto = set()
    resultados = []

    def _agregar(hora_fmt, animal):
        k = hora_fmt
        if k not in visto:
            visto.add(k)
            resultados.append({"fecha": hoy, "hora": hora_fmt,
                                "animalito": _normalizar(animal)})

    # ── Estrategia 1: divs / li / article ──
    for bloque in soup.find_all(["div", "li", "article", "section", "p"]):
        txt = bloque.get_text(separator=" ", strip=True)
        mh = _HORA_RE.search(txt)
        ma = _ANIMALES_RE.search(txt)
        if mh and ma:
            _agregar(_fmt_hora(mh.group(1), mh.group(2)), ma.group(1))

    # ── Estrategia 2: filas <tr> ──
    if not resultados:
        for row in soup.find_all("tr"):
            txt = row.get_text(separator=" ", strip=True)
            mh = _HORA_RE.search(txt)
            if not mh:
                continue
            ma = _ANIMALES_RE.search(txt)
            if ma:
                _agregar(_fmt_hora(mh.group(1), mh.group(2)), ma.group(1))
            else:
                # intentar por número
                mn = re.search(r'\b(\d{1,2})\b', txt)
                if mn and mn.group(1) in NUM_A_ANIMAL:
                    _agregar(_fmt_hora(mh.group(1), mh.group(2)),
                             NUM_A_ANIMAL[mn.group(1)])

    # ── Estrategia 3: texto bruto completo ──
    if not resultados:
        todo = soup.get_text(separator="\n")
        for linea in todo.splitlines():
            mh = _HORA_RE.search(linea)
            ma = _ANIMALES_RE.search(linea)
            if mh and ma:
                _agregar(_fmt_hora(mh.group(1), mh.group(2)), ma.group(1))

    logger.info(f"[Scraper] Hoy ({hoy}): {len(resultados)} sorteos")
    return resultados


async def obtener_historico_semana(fecha_inicio, fecha_fin):
    """Histórico semanal de loteriadehoy.com"""
    try:
        async with httpx.AsyncClient(
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"},
            follow_redirects=True,
        ) as client:
            r = await client.get(f"{BASE_URL}/historico/{fecha_inicio}/{fecha_fin}/")
            r.raise_for_status()
    except Exception as e:
        logger.error(f"[Scraper] Histórico error: {e}")
        return []

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(r.text, "html.parser")
    resultados = []
    tabla = soup.find("table")
    if not tabla:
        return []

    # Leer fechas de los encabezados
    ths = tabla.find_all("th")
    fechas_col = []
    for th in ths[1:]:
        m = re.search(r'(\d{4}-\d{2}-\d{2})', th.get_text())
        if m:
            try:
                fechas_col.append(date.fromisoformat(m.group(1)))
            except Exception:
                fechas_col.append(None)
        else:
            fechas_col.append(None)

    for tr in tabla.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        mh = _HORA_RE.search(tds[0].get_text())
        if not mh:
            continue
        hora_fmt = _fmt_hora(mh.group(1), mh.group(2))

        for ci, td in enumerate(tds[1:]):
            if ci >= len(fechas_col) or not fechas_col[ci]:
                continue
            txt = td.get_text(strip=True)
            ma = _ANIMALES_RE.search(txt)
            if ma:
                resultados.append({
                    "fecha": fechas_col[ci],
                    "hora": hora_fmt,
                    "animalito": _normalizar(ma.group(1)),
                })
            else:
                mn = re.search(r'\b(\d{1,2})\b', txt)
                if mn and mn.group(1) in NUM_A_ANIMAL:
                    resultados.append({
                        "fecha": fechas_col[ci],
                        "hora": hora_fmt,
                        "animalito": NUM_A_ANIMAL[mn.group(1)],
                    })

    logger.info(f"[Scraper] Histórico {fecha_inicio}→{fecha_fin}: {len(resultados)} reg.")
    return resultados


# ═══════════════════════════════════════════════════════════════
# BD — guardar + actualizar auditoria_ia
# ═══════════════════════════════════════════════════════════════

async def _col_hora_historico(db) -> str:
    """Detecta si la tabla historico usa 'hora_sorteo' o 'hora'."""
    from sqlalchemy import text
    try:
        r = (await db.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='historico' AND column_name='hora_sorteo'"
        ))).fetchone()
        return "hora_sorteo" if r else "hora"
    except Exception:
        return "hora_sorteo"


async def guardar_resultados(db, resultados: list) -> int:
    from sqlalchemy import text
    if not resultados:
        return 0

    col = await _col_hora_historico(db)
    nuevos = 0

    for r in resultados:
        try:
            res = await db.execute(text(f"""
                INSERT INTO historico (fecha, {col}, animalito, loteria)
                VALUES (:f, :h, :a, 'Lotto Activo')
                ON CONFLICT (fecha, {col}) DO NOTHING
            """), {"f": r["fecha"], "h": r["hora"], "a": r["animalito"]})
            if res.rowcount > 0:
                nuevos += 1
        except Exception:
            continue

    if nuevos > 0:
        await db.commit()

    # ── CRÍTICO: actualizar resultado_real y acierto en auditoria_ia ──
    actualizados = 0
    for r in resultados:
        try:
            upd = await db.execute(text("""
                UPDATE auditoria_ia
                SET
                    resultado_real = CAST(:animal AS VARCHAR),
                    acierto = (
                        LOWER(TRIM(COALESCE(animal_predicho, ''))) = LOWER(TRIM(CAST(:animal AS VARCHAR)))
                        OR LOWER(TRIM(COALESCE(prediccion_1,  ''))) = LOWER(TRIM(CAST(:animal AS VARCHAR)))
                    )
                WHERE fecha = :fecha
                  AND LOWER(TRIM(hora)) = LOWER(TRIM(CAST(:hora AS VARCHAR)))
                  AND resultado_real IS NULL
            """), {
                "animal": r["animalito"],
                "fecha":  r["fecha"],
                "hora":   r["hora"],
            })
            actualizados += upd.rowcount
        except Exception as ex:
            logger.warning(f"[BD] auditoria_ia update error {r}: {ex}")
            await db.rollback()  # FIX: limpiar tx rota

    if actualizados > 0:
        await db.commit()
        logger.info(f"[BD] ✅ {actualizados} filas de auditoria_ia actualizadas")

    return nuevos


async def actualizar_auditoria_pendiente(db):
    """
    Repara TODAS las filas donde resultado_real IS NULL pero el dato
    ya existe en historico. Soluciona el bug de PEND. masivo.
    """
    from sqlalchemy import text
    col = await _col_hora_historico(db)
    try:
        upd = await db.execute(text(f"""
            UPDATE auditoria_ia a
            SET
                resultado_real = h.animalito,
                acierto = (
                    LOWER(TRIM(COALESCE(a.animal_predicho,''))) = LOWER(TRIM(h.animalito))
                    OR LOWER(TRIM(COALESCE(a.prediccion_1, ''))) = LOWER(TRIM(h.animalito))
                )
            FROM historico h
            WHERE h.fecha = a.fecha
              AND LOWER(TRIM(h.{col})) = LOWER(TRIM(a.hora))
              AND h.loteria = 'Lotto Activo'
              AND a.resultado_real IS NULL
              AND h.animalito IS NOT NULL
        """))
        if upd.rowcount > 0:
            await db.commit()
            logger.info(f"[BD] 🔁 {upd.rowcount} predicciones PEND. corregidas")
        return upd.rowcount
    except Exception as e:
        logger.warning(f"[BD] actualizar_pendiente error: {e}")
        await db.rollback()
        return 0


# ═══════════════════════════════════════════════════════════════
# CAPTURA Y PROCESAMIENTO
# ═══════════════════════════════════════════════════════════════

def _importar_motor():
    """Importa funciones del motor disponible (v9 → v10 → fallback vacío)."""
    for modulo in ("app.services.motor_v9", "app.services.motor_v10",
                   "app.services.motor_v5"):
        try:
            import importlib
            m = importlib.import_module(modulo)
            logger.info(f"[Motor] Usando {modulo}")
            return (
                getattr(m, "calibrar_predicciones",   None),
                getattr(m, "entrenar_modelo",          None),
                getattr(m, "generar_prediccion",       None),
                getattr(m, "aprender_desde_historico", None),
            )
        except ImportError:
            continue
    logger.error("[Motor] No se encontró ningún módulo de motor")
    return None, None, None, None


async def capturar_y_procesar(db) -> bool:
    calibrar, entrenar, predecir, _ = _importar_motor()

    resultados = await obtener_resultados_hoy()
    nuevos = await guardar_resultados(db, resultados)

    if nuevos > 0:
        logger.info(f"✅ {nuevos} nuevo(s) resultado(s) capturado(s)")
        if calibrar:
            try:
                cal = await calibrar(db)
                logger.info(f"🎯 Calibración: {cal.get('calibradas', 0)} validadas")
            except Exception as e:
                logger.warning(f"[Scheduler] calibrar error: {e}")
        if entrenar:
            try:
                await entrenar(db)
            except Exception as e:
                logger.warning(f"[Scheduler] entrenar error: {e}")
    else:
        # Sin nuevos en historico: aun así intentar reparar pendientes
        await actualizar_auditoria_pendiente(db)

    if predecir:
        try:
            pred = await predecir(db)
            top1 = pred.get("top3", [{}])[0]
            logger.info(
                f"🔮 Predicción: {top1.get('animal','?')} "
                f"({top1.get('porcentaje','?')}) | "
                f"Conf: {pred.get('confianza_idx', 0)}/100"
            )
        except Exception as e:
            logger.warning(f"[Scheduler] predecir error: {e}")

    return nuevos > 0


# ═══════════════════════════════════════════════════════════════
# APRENDIZAJE SEMANAL
# ═══════════════════════════════════════════════════════════════

async def aprendizaje_semanal(db):
    _, _, _, aprender = _importar_motor()
    if not aprender:
        return {"message": "Motor no disponible"}
    logger.info("🧠 Aprendizaje semanal iniciado...")
    hoy = hora_venezuela().date()
    res = await aprender(db, hoy - timedelta(days=90))
    logger.info(f"🧠 Aprendizaje semanal: {res.get('message','')}")
    return res


# ═══════════════════════════════════════════════════════════════
# CARGA INICIAL
# ═══════════════════════════════════════════════════════════════

async def carga_inicial(db):
    logger.info("📦 Carga inicial — últimos 14 días...")
    total = 0
    hoy = hora_venezuela().date()

    for offset in range(0, 14, 7):
        fecha_fin = hoy - timedelta(days=offset)
        fecha_ini = fecha_fin - timedelta(days=6)
        r = await obtener_historico_semana(fecha_ini, fecha_fin)
        total += await guardar_resultados(db, r)
        await asyncio.sleep(1)

    r_hoy = await obtener_resultados_hoy()
    nuevos_hoy = await guardar_resultados(db, r_hoy)
    total += nuevos_hoy

    # REPARAR todas las filas PEND. que ya existen en historico
    reparados = await actualizar_auditoria_pendiente(db)

    logger.info(
        f"✅ Carga inicial: {total} nuevos ({nuevos_hoy} hoy) | "
        f"{reparados} PEND. reparados"
    )
    return total


# ═══════════════════════════════════════════════════════════════
# CICLO PRINCIPAL
# ═══════════════════════════════════════════════════════════════

async def ciclo_infinito():
    from db import get_db as _get_db
    calibrar, entrenar, predecir, _ = _importar_motor()

    await asyncio.sleep(3)

    # ── Inicialización ──
    async for db in _get_db():
        try:
            await carga_inicial(db)
            if entrenar:
                res = await entrenar(db)
                logger.info(f"🧠 {res.get('message', 'Entrenamiento OK')}")
            if calibrar:
                cal = await calibrar(db)
                logger.info(f"🎯 Calibración: {cal.get('calibradas', 0)} validadas")
            if predecir:
                pred = await predecir(db)
                top1 = pred.get("top3", [{}])[0]
                logger.info(
                    f"🔮 Pred inicial: {top1.get('animal','?')} "
                    f"({top1.get('porcentaje','?')}) — "
                    f"Conf: {pred.get('confianza_idx', 0)}/100"
                )
        except Exception as e:
            logger.error(f"Error carga inicial: {e}")
        break

    intentos_fallidos = 0
    ultimo_aprendizaje  = hora_venezuela().date()
    ultima_rep_pendiente = hora_venezuela()

    ultimo_ping = hora_venezuela()

    while True:
        try:
            ahora         = hora_venezuela()
            hora_actual   = ahora.hour
            minuto_actual = ahora.minute
            dia_semana    = ahora.weekday()   # 6 = domingo

            # ── Self-ping cada 8 min para evitar que Render duerma ──
            if (ahora - ultimo_ping).total_seconds() > 240:  # ping cada 4 min
                try:
                    import httpx
                    # RENDER_EXTERNAL_URL debe estar en env vars de Render
                    # Valor: https://api-lotto-goj5.onrender.com
                    render_url = os.getenv("RENDER_EXTERNAL_URL", "https://api-lotto-goj5.onrender.com")
                    async with httpx.AsyncClient(timeout=10) as client:
                        await client.get(f"{render_url}/health")
                    logger.info(f"💓 Self-ping OK → {render_url}")
                except Exception as pe:
                    logger.debug(f"Self-ping skip: {pe}")
                ultimo_ping = ahora

            # ── Reparar PEND. cada 30 min (aunque no sea hora de sorteo) ──
            if (ahora - ultima_rep_pendiente).total_seconds() > 1800:
                async for db in _get_db():
                    await actualizar_auditoria_pendiente(db)
                    break
                ultima_rep_pendiente = ahora

            # ── Aprendizaje semanal (domingo 3AM) ──
            if (dia_semana == 6 and hora_actual == 3
                    and minuto_actual < 5
                    and ahora.date() != ultimo_aprendizaje):
                async for db in _get_db():
                    await aprendizaje_semanal(db)
                    ultimo_aprendizaje = ahora.date()
                    break

            # ── Ventana de captura: minutos 3-25 de cada hora de sorteo ──
            if hora_actual in HORAS_SORTEO and 3 <= minuto_actual <= 25:
                logger.info(
                    f"🔍 [{ahora.strftime('%H:%M')}] "
                    f"Buscando sorteo {hora_actual}:00..."
                )
                async for db in _get_db():
                    encontrado = await capturar_y_procesar(db)
                    if encontrado:
                        intentos_fallidos = 0
                        espera = (65 - minuto_actual) * 60
                        logger.info(f"⏰ Próxima revisión en {espera//60} min")
                        await asyncio.sleep(espera)
                    else:
                        intentos_fallidos += 1
                        if intentos_fallidos >= 5:
                            logger.warning(
                                f"⚠️ {intentos_fallidos} intentos sin captura. "
                                "Saltando a siguiente hora."
                            )
                            intentos_fallidos = 0
                            await asyncio.sleep(40 * 60)
                        else:
                            logger.info(
                                f"⏳ Sin resultado "
                                f"(intento {intentos_fallidos}/5). "
                                "Reintento en 3 min."
                            )
                            await asyncio.sleep(180)
                    break

            else:
                # ── Fuera de ventana: calcular espera ──
                if hora_actual in HORAS_SORTEO:
                    if minuto_actual < 3:
                        espera = (3 - minuto_actual) * 60
                    else:
                        prox = hora_actual + 1
                        espera = ((60 - minuto_actual + 3) * 60
                                  if prox in HORAS_SORTEO else 300)
                elif hora_actual < 8:
                    espera = max((8 - hora_actual) * 3600
                                 - minuto_actual * 60 + 180, 60)
                elif hora_actual >= 20:
                    # Captura final post-día
                    async for db in _get_db():
                        await capturar_y_procesar(db)
                        break
                    espera = max((32 - hora_actual) * 3600, 3600)
                else:
                    espera = 300

                logger.info(
                    f"⏰ [{ahora.strftime('%H:%M')}] "
                    f"Esperando ventana (~{espera//60} min)."
                )
                await asyncio.sleep(espera)

        except Exception as e:
            logger.error(f"Error en ciclo: {e}")
            await asyncio.sleep(60)
