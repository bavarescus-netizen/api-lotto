import math
from collections import Counter, defaultdict

ANIMALES = list(range(1, 39))


# --------------------------------
# NORMALIZACIÓN DE DEUDA
# --------------------------------

def score_deuda(dias_ausente, ciclo_promedio):

    if ciclo_promedio <= 0:
        return 0

    ratio = dias_ausente / ciclo_promedio

    return math.log1p(ratio)


# --------------------------------
# FRECUENCIA VS AZAR
# --------------------------------

def score_frecuencia(freq, total):

    if total == 0:
        return 0

    prob_real = freq / total
    prob_azar = 1 / 38

    ratio = prob_real / prob_azar

    return math.log1p(ratio)


# --------------------------------
# MARKOV SIMPLE
# --------------------------------

def calcular_markov(historial):

    transiciones = defaultdict(Counter)

    for i in range(1, len(historial)):

        anterior = historial[i - 1]
        actual = historial[i]

        transiciones[anterior][actual] += 1

    return transiciones


def prob_markov(historial):

    if len(historial) < 2:
        return {}

    anterior = historial[-1]

    transiciones = calcular_markov(historial)

    if anterior not in transiciones:
        return {}

    total = sum(transiciones[anterior].values())

    probs = {}

    for animal, count in transiciones[anterior].items():

        probs[animal] = count / total

    return probs


# --------------------------------
# PENALIZACIÓN DIVERSIDAD
# --------------------------------

def penalizacion_diversidad(veces):

    return math.exp(-0.2 * veces)


# --------------------------------
# CALCULAR GAP
# --------------------------------

def calcular_gap(historial):

    gap = {}

    for animal in ANIMALES:

        gap[animal] = 0

        for i in range(len(historial) - 1, -1, -1):

            if historial[i] == animal:
                break

            gap[animal] += 1

    return gap


# --------------------------------
# CICLO PROMEDIO
# --------------------------------

def ciclo_promedio(historial):

    ciclos = {a: [] for a in ANIMALES}

    ultima = {}

    for i, animal in enumerate(historial):

        if animal in ultima:

            ciclo = i - ultima[animal]
            ciclos[animal].append(ciclo)

        ultima[animal] = i

    promedio = {}

    for a in ANIMALES:

        if len(ciclos[a]) == 0:
            promedio[a] = 10
        else:
            promedio[a] = sum(ciclos[a]) / len(ciclos[a])

    return promedio


# --------------------------------
# SCORE PRINCIPAL
# --------------------------------

def calcular_scores(historial, historial_predicciones=None):

    total = len(historial)

    freq = Counter(historial)

    gap = calcular_gap(historial)

    ciclos = ciclo_promedio(historial)

    markov = prob_markov(historial)

    scores = {}

    for animal in ANIMALES:

        s_deuda = score_deuda(gap[animal], ciclos[animal])

        s_freq = score_frecuencia(freq[animal], total)

        s_markov = markov.get(animal, 0)

        score = (
            0.35 * s_deuda +
            0.35 * s_freq +
            0.30 * s_markov
        )

        if historial_predicciones:

            veces = historial_predicciones.count(animal)
            score *= penalizacion_diversidad(veces)

        scores[animal] = score

    return scores


# --------------------------------
# TOP PREDICCIÓN
# --------------------------------

def generar_prediccion(historial, historial_predicciones=None):

    scores = calcular_scores(historial, historial_predicciones)

    ordenados = sorted(
        scores.items(),
        key=lambda x: x[1],
        reverse=True
    )

    top5 = [a for a, _ in ordenados[:5]]

    return {
        "top1": top5[0],
        "top3": top5[:3],
        "top5": top5,
        "scores": scores
    }
