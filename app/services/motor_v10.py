import math
from collections import Counter, defaultdict

ANIMALES = list(range(1, 39))


# --------------------------------
# DEUDA (CORREGIDA)
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

        if len(ciclos[animal]) == 0:
            ciclo_prom = 10
        else:
            ciclo_prom = sum(ciclos[animal]) / len(ciclos[animal])

        ratio = dias / ciclo_prom if ciclo_prom > 0 else 0

        # 🔥 CORRECCIÓN
        gap[animal] = math.log1p(ratio)

    return gap


# --------------------------------
# FRECUENCIA (CORREGIDA)
# --------------------------------

def calcular_frecuencia_reciente(historial):

    total = len(historial)
    freq = Counter(historial)

    scores = {}

    for a in ANIMALES:

        prob_real = freq[a] / total if total > 0 else 0
        prob_azar = 1 / 38

        # 🔥 CORRECCIÓN
        scores[a] = math.log1p(prob_real / prob_azar)

    return scores


# --------------------------------
# ANTI RACHA (CORREGIDA)
# --------------------------------

def calcular_anti_racha(historial):

    dias = {}

    for a in ANIMALES:
        dias[a] = 0

        for i in range(len(historial)-1, -1, -1):
            if historial[i] == a:
                break
            dias[a] += 1

        # 🔥 CORRECCIÓN (menos agresivo)
        dias[a] = math.exp(-dias[a] * 0.3)

    return dias


# --------------------------------
# MARKOV POR HORA (ESTABLE)
# --------------------------------

def calcular_markov_hora(historial):

    transiciones = defaultdict(Counter)

    for i in range(1, len(historial)):
        prev = historial[i-1]
        curr = historial[i]
        transiciones[prev][curr] += 1

    probs = {}

    if len(historial) < 2:
        return probs

    ultimo = historial[-1]

    if ultimo not in transiciones:
        return probs

    total = sum(transiciones[ultimo].values())

    for a, c in transiciones[ultimo].items():
        probs[a] = c / total

    return probs


# --------------------------------
# PATRÓN DÍA (SE MANTIENE SIMPLE)
# --------------------------------

def calcular_patron_dia(historial):

    freq = Counter(historial)

    total = len(historial)

    return {a: freq[a] / total if total > 0 else 0 for a in ANIMALES}


# --------------------------------
# COMBINAR SEÑALES (CORREGIDO)
# --------------------------------

def combinar_señales(
    deuda,
    frecuencia,
    patron,
    anti,
    markov,
    historial_predicciones=None
):

    scores = {}

    for a in ANIMALES:

        s_deuda = deuda.get(a, 0)
        s_freq = frecuencia.get(a, 0)
        s_patron = patron.get(a, 0)
        s_anti = anti.get(a, 0)
        s_markov = markov.get(a, 0)

        # 🔥 CORRECCIÓN DE PESOS
        score = (
            0.30 * s_deuda +
            0.30 * s_freq +
            0.15 * s_patron +
            0.10 * s_anti +
            0.15 * s_markov
        )

        # 🔥 PENALIZACIÓN SUAVE
        if historial_predicciones:
            veces = historial_predicciones.count(a)
            score *= math.exp(-0.15 * veces)

        scores[a] = score

    return scores


# --------------------------------
# GENERAR PREDICCIÓN (NO CAMBIA)
# --------------------------------

def generar_prediccion(historial, historial_predicciones=None):

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
        historial_predicciones
    )

    ordenados = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    top5 = [a for a, _ in ordenados[:5]]

    return {
        "top1": top5[0],
        "top3": top5[:3],
        "top5": top5,
        "scores": scores
    }


# --------------------------------
# ESTADÍSTICAS (SE MANTIENE)
# --------------------------------

def obtener_estadisticas(historial):

    total = len(historial)

    freq = Counter(historial)

    top = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:10]

    return {
        "total_sorteos": total,
        "top_animales": top
    }
