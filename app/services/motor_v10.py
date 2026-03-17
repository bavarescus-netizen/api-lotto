import math
from collections import Counter, defaultdict

# 🔥 IMPORTANTE (modelo híbrido)
try:
    from app.services.modelo_rf import ModeloRF
    modelo_rf = ModeloRF()
except:
    modelo_rf = None

ANIMALES = list(range(1, 39))

# --------------------------------
# BITÁCORA (SOLUCIÓN ERROR)
# --------------------------------

bitacora_global = []

def obtener_bitacora():
    return bitacora_global

def guardar_en_bitacora(prediccion, resultado_real=None):
    registro = {
        "prediccion": prediccion,
        "resultado_real": resultado_real
    }
    bitacora_global.append(registro)

    if len(bitacora_global) > 1000:
        bitacora_global.pop(0)

# --------------------------------
# DEUDA
# --------------------------------

def calcular_deuda(historial):
    gap = {}
    ciclos = {a: [] for a in ANIMALES}
    ultima = {}

    for i, animal in enumerate(historial):
        if animal in ultima:
            ciclos[animal].append(i - ultima[animal])
        ultima[animal] = i

    for animal in ANIMALES:
        dias = 0
        for i in range(len(historial)-1, -1, -1):
            if historial[i] == animal:
                break
            dias += 1

        ciclo_prom = sum(ciclos[animal])/len(ciclos[animal]) if ciclos[animal] else 10
        ratio = dias / ciclo_prom if ciclo_prom > 0 else 0

        gap[animal] = math.log1p(ratio)

    return gap

# --------------------------------
# FRECUENCIA
# --------------------------------

def calcular_frecuencia_reciente(historial):
    total = len(historial)
    freq = Counter(historial)

    scores = {}

    for a in ANIMALES:
        prob_real = freq[a] / total if total > 0 else 0
        prob_azar = 1 / 38
        scores[a] = math.log1p(prob_real / prob_azar)

    return scores

# --------------------------------
# ANTI RACHA
# --------------------------------

def calcular_anti_racha(historial):
    dias = {}

    for a in ANIMALES:
        dias[a] = 0

        for i in range(len(historial)-1, -1, -1):
            if historial[i] == a:
                break
            dias[a] += 1

        dias[a] = math.exp(-dias[a] * 0.3)

    return dias

# --------------------------------
# MARKOV
# --------------------------------

def calcular_markov_hora(historial):
    transiciones = defaultdict(Counter)

    for i in range(1, len(historial)):
        transiciones[historial[i-1]][historial[i]] += 1

    if len(historial) < 2:
        return {}

    ultimo = historial[-1]

    if ultimo not in transiciones:
        return {}

    total = sum(transiciones[ultimo].values())

    return {a: c / total for a, c in transiciones[ultimo].items()}

# --------------------------------
# PATRÓN
# --------------------------------

def calcular_patron_dia(historial):
    freq = Counter(historial)
    total = len(historial)

    return {a: freq[a] / total if total > 0 else 0 for a in ANIMALES}

# --------------------------------
# COMBINADOR (HÍBRIDO)
# --------------------------------

def combinar_señales(
    deuda,
    frecuencia,
    patron,
    anti,
    markov,
    historial_predicciones=None,
    rf_probs=None
):

    scores = {}

    for a in ANIMALES:

        s_deuda = deuda.get(a, 0)
        s_freq = frecuencia.get(a, 0)
        s_patron = patron.get(a, 0)
        s_anti = anti.get(a, 0)
        s_markov = markov.get(a, 0)
        s_rf = rf_probs.get(a, 0) if rf_probs else 0

        score = (
            0.25 * s_deuda +
            0.25 * s_freq +
            0.15 * s_patron +
            0.10 * s_anti +
            0.10 * s_markov +
            0.15 * s_rf
        )

        if historial_predicciones:
            veces = historial_predicciones.count(a)
            score *= math.exp(-0.15 * veces)

        scores[a] = score

    return scores

# --------------------------------
# PREDICCIÓN
# --------------------------------

def generar_prediccion(historial, historial_predicciones=None):

    # 🔥 IA (si está disponible)
    rf_probs = {}
    if modelo_rf:
        try:
            modelo_rf.entrenar(historial)
            rf_probs = modelo_rf.predecir(historial)
        except:
            rf_probs = {}

    deuda = calcular_deuda(historial)
    frecuencia = calcular_frecuencia_reciente(historial)
    patron = calcular_patron_dia(historial)
    anti = calcular_anti_racha(historial)
    markov = calcular_markov_hora(historial)

    scores = combinar_señales(
        deuda,
        frecuencia,
        patron,
        anti,
        markov,
        historial_predicciones,
        rf_probs
    )

    ordenados = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top5 = [a for a, _ in ordenados[:5]]

    # 🔥 guardar en bitácora
    guardar_en_bitacora(top5)

    return {
        "top1": top5[0],
        "top3": top5[:3],
        "top5": top5,
        "scores": scores
    }

# --------------------------------
# ESTADÍSTICAS
# --------------------------------

def obtener_estadisticas(historial):
    total = len(historial)
    freq = Counter(historial)

    top = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:10]

    return {
        "total_sorteos": total,
        "top_animales": top
    }
