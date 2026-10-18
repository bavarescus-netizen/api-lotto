"""
Pruebas de azar sobre historico + diagnostico de aciertos recientes.
Uso:  pip install psycopg2-binary pandas scipy
      set DATABASE_URL=postgresql://...   (o export en Linux/Mac)
      python prueba_azar.py
Solo lee (SELECT). No modifica nada.
"""
import os, sys
import numpy as np
import pandas as pd
from scipy import stats
import psycopg2

DSN = os.environ.get("DATABASE_URL") or sys.exit("Define DATABASE_URL")
con = psycopg2.connect(DSN)

def leer(sql):
    return pd.read_sql(sql, con)

def orden_hora(s):
    return pd.to_datetime(s.str.strip(), format="%I:%M %p", errors="coerce")

# ---------------------------------------------------------------- historico
h = leer("""SELECT fecha, hora, LOWER(TRIM(animalito)) AS animal
            FROM historico WHERE COALESCE(animalito,'') <> ''""")
h = h.drop_duplicates(["fecha", "hora"], keep="last")
h["t"] = orden_hora(h["hora"])
h = h.dropna(subset=["t"]).sort_values(["fecha", "t"]).reset_index(drop=True)
animales = sorted(h["animal"].unique())
K = len(animales)
print(f"Sorteos: {len(h)} | animales distintos: {K} | {h.fecha.min()} a {h.fecha.max()}\n")

def veredicto(p, alfa=0.05):
    return "DESVIACION significativa" if p < alfa else "compatible con azar"

# 1) Uniformidad global
c = h["animal"].value_counts().reindex(animales, fill_value=0)
chi, p = stats.chisquare(c)
print(f"[1] Uniformidad global por animal: chi2={chi:.1f} gl={K-1} p={p:.4f} -> {veredicto(p)}")
print("    mas frecuentes :", ", ".join(f"{a} {v/len(h)*100:.2f}%" for a, v in c.nlargest(4).items()))
print("    menos frecuentes:", ", ".join(f"{a} {v/len(h)*100:.2f}%" for a, v in c.nsmallest(4).items()))
print(f"    (esperado por animal: {100/K:.2f}%)\n")

# 2) Uniformidad por hora (correccion Bonferroni)
print("[2] Uniformidad por hora (p ajustado x nº de horas):")
horas = sorted(h["hora"].unique(), key=lambda x: pd.to_datetime(x.strip(), format="%I:%M %p"))
for hr in horas:
    ch = h[h.hora == hr]["animal"].value_counts().reindex(animales, fill_value=0)
    chi, p = stats.chisquare(ch)
    padj = min(1, p * len(horas))
    print(f"    {hr:9s} n={ch.sum():5d} p={p:.4f} p_ajust={padj:.4f} -> {veredicto(padj)}")
print()

# 3) Independencia entre sorteos consecutivos del mismo dia
h["prev"] = h.groupby("fecha")["animal"].shift()
par = h.dropna(subset=["prev"])
ct = pd.crosstab(par["prev"], par["animal"])
chi, p, gl, _ = stats.chi2_contingency(ct)
print(f"[3] Independencia sorteo->siguiente (mismo dia): chi2={chi:.1f} gl={gl} p={p:.4f} -> {veredicto(p)}")
rep = (par["prev"] == par["animal"]).sum()
bt = stats.binomtest(int(rep), len(par), 1 / K)
print(f"    repite el animal anterior: {rep}/{len(par)} = {rep/len(par)*100:.2f}% (azar {100/K:.2f}%) p={bt.pvalue:.4f}\n")

# 4) Mismo animal a la misma hora del dia siguiente
h["prev_h"] = h.groupby("hora")["animal"].shift()
ph = h.dropna(subset=["prev_h"])
rep = (ph["prev_h"] == ph["animal"]).sum()
bt = stats.binomtest(int(rep), len(ph), 1 / K)
print(f"[4] Repite el animal de la misma hora del dia anterior: {rep}/{len(ph)} = {rep/len(ph)*100:.2f}% "
      f"(azar {100/K:.2f}%) p={bt.pvalue:.4f}\n")

# 5) Repeticiones dentro del dia (animal que sale 2+ veces el mismo dia)
dia = h.groupby("fecha")["animal"].agg(lambda s: len(s) - s.nunique())
full = h.groupby("fecha").size()
sim = []
rng = np.random.default_rng(1)
for _ in range(2000):
    sim.append(np.mean([12 - len(set(rng.integers(0, K, 12))) for _ in range(300)]))
obs = dia[full == 12].mean()
print(f"[5] Repeticiones por dia completo (12 sorteos): observado {obs:.3f} | azar {np.mean(sim):.3f} "
      f"(sd {np.std(sim):.3f}) -> z={(obs-np.mean(sim))/np.std(sim):.2f}\n")

# ------------------------------------------------- predicciones en vivo
v = leer("""SELECT a.fecha, a.hora, LOWER(TRIM(a.prediccion_1)) p1, LOWER(TRIM(a.prediccion_2)) p2,
                   LOWER(TRIM(a.prediccion_3)) p3, LOWER(TRIM(h.animalito)) real
            FROM auditoria_ia a JOIN historico h ON h.fecha=a.fecha AND h.hora=a.hora
            WHERE a.origen <> 'INTRADAY' AND COALESCE(h.animalito,'') <> ''""")
v = v.drop_duplicates(["fecha", "hora"], keep="last")
v["t"] = orden_hora(v["hora"])
v = v.sort_values(["fecha", "t"]).reset_index(drop=True)
v["ok"] = (v["real"] == v["p1"]) | (v["real"] == v["p2"]) | (v["real"] == v["p3"])
n, k = len(v), int(v["ok"].sum())
p0 = 3 / K
print(f"[6] Predicciones en vivo: {n} sorteos, {k} aciertos top3 = {k/n*100:.2f}% (azar {p0*100:.2f}%) "
      f"p={stats.binomtest(k, n, p0).pvalue:.4f}")

# 6) Racha de aciertos / fallos: Wald-Wolfowitz
x = v["ok"].astype(int).values
n1, n0 = x.sum(), len(x) - x.sum()
runs = 1 + np.sum(x[1:] != x[:-1])
mu = 1 + 2 * n1 * n0 / len(x)
var = 2 * n1 * n0 * (2 * n1 * n0 - len(x)) / (len(x) ** 2 * (len(x) - 1))
z = (runs - mu) / np.sqrt(var)
print(f"    Test de rachas: {runs} rachas, esperadas {mu:.1f}, z={z:.2f}, p={2*(1-stats.norm.cdf(abs(z))):.4f} "
      "(si p alto: los aciertos NO vienen agrupados mas de lo normal)")

# 7) Distribucion de aciertos por dia vs binomial
d = v.groupby("fecha")["ok"].agg(["sum", "count"])
dc = d[d["count"] == 12]
obs = pd.cut(dc["sum"], [-1, 0, 1, 2, 99], labels=["0", "1", "2", "3+"]).value_counts(normalize=True).reindex(["0", "1", "2", "3+"])
b = stats.binom(12, p0)
esp = [b.pmf(0), b.pmf(1), b.pmf(2), 1 - b.cdf(2)]
print(f"\n[7] Aciertos por dia completo (n={len(dc)} dias), observado vs esperado por azar:")
for lab, o, e in zip(["0", "1", "2", "3+"], obs.values, esp):
    print(f"    {lab:>2} aciertos: observado {o*100:5.1f}% | azar {e*100:5.1f}%")

# 8) Ultimos 14 dias
print("\n[8] Aciertos por dia (ultimos 14 dias):")
print(d.tail(14).rename(columns={"sum": "aciertos", "count": "sorteos"}).to_string())

# 9) Detalle ultimos 5 dias
print("\n[9] Detalle ultimos 5 dias (OK = real dentro del top3):")
ult = sorted(v["fecha"].unique())[-5:]
for f in ult:
    print(f"  {f}")
    for _, r in v[v.fecha == f].iterrows():
        print(f"    {r.hora:9s} pred: {r.p1}/{r.p2}/{r.p3:<10s} salio: {r.real:<10s} {'OK' if r.ok else '--'}")

# 10) Que animales sobre-predice vs lo que sale (ventana viva)
pred = pd.concat([v.p1, v.p2, v.p3]).value_counts() / (3 * n)
real = v["real"].value_counts() / n
cmp = pd.DataFrame({"pred_%": pred * 100, "real_%": real * 100}).fillna(0)
cmp["dif"] = cmp["pred_%"] - cmp["real_%"]
print("\n[10] Animales mas sobre-predichos vs su frecuencia real (ventana viva):")
print(cmp.sort_values("dif", ascending=False).head(8).round(2).to_string())
print("\n    Animales que mas salen y menos se predicen:")
print(cmp.sort_values("dif").head(8).round(2).to_string())
con.close()
