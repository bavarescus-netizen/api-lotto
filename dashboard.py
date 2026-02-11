import streamlit as st
import httpx
import pandas as pd
import matplotlib.pyplot as plt

API = "https://api-lotto-t6p5.onrender.com"


st.title("📊 Lotto AI Dashboard PRO")

# =====================
# BOTONES
# =====================

if st.button("Entrenar modelo"):
    r = httpx.get(f"{API}/entrenar/")
    st.success(r.json())

if st.button("Ver predicción"):
    r = httpx.get(f"{API}/prediccion")
    st.json(r.json())


# =====================
# ESTADÍSTICAS
# =====================

r = httpx.get(f"{API}/stats")
data = r.json()

st.subheader("Top animales globales")
st.write(data["freq_global"])


# =====================
# GRÁFICO precisión por hora
# =====================

horas = []
precisiones = []

for h, v in data["hora_stats"].items():
    horas.append(h)
    precisiones.append(v["precision"])

df = pd.DataFrame({
    "hora": horas,
    "precision": precisiones
})

fig = plt.figure()
plt.bar(df["hora"], df["precision"])
plt.xticks(rotation=90)
plt.ylabel("Precisión")
plt.title("Precisión por hora")

st.pyplot(fig)
