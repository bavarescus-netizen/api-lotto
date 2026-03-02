import httpx
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta, date
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db import get_db
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

BASE_URL = "https://loteriadehoy.com/animalito/lottoactivo"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
LOTERIA = "Lotto Activo"


def normalizar_animal(texto):
    if not texto:
        return ""
    limpio = re.sub(r'[^a-zA-ZáéíóúñÁÉÍÓÚÑ\s]', '', texto)
    partes = limpio.strip().split()
    return partes[-1].lower() if partes else ""


def normalizar_hora(hora_texto):
    hora = hora_texto.strip().upper()
    partes = hora.split(':')
    if len(partes) >= 2:
        return f"{partes[0].zfill(2)}:{':'.join(partes[1:])}"
    return hora


async def obtener_resultados_hoy():
    url = f"{BASE_URL}/resultados/"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, headers=HEADERS)
            if r.status_code != 200:
                return []
            soup = BeautifulSoup(r.text, "html.parser")
            fecha_hoy = date.today()
            resultados = []
            for leyenda in soup.find_all("div", class_="circle-legend"):
                h4 = leyenda.find("h4")
                h5 = leyenda.find("h5")
                if not h4 or not h5:
                    continue
                animal = normalizar_animal(h4.text)
                match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)', h5.text, re.IGNORECASE)
                if not match or not animal:
                    continue
                hora = normalizar_hora(match.group(1))
                resultados.append({
                    "fecha": fecha_hoy,
                    "hora": hora,
                    "animalito": animal,
                    "loteria": LOTERIA
                })
            logger.info(f"Hoy: {len(resultados)} sorteos encontrados")
            return resultados
    except Exception as e:
        logger.error(f"Error scraper hoy: {e}")
        return []


async def obtener_historico_semana(fecha_inicio: date, fecha_fin: date):
    url = f"{BASE_URL}/historico/{fecha_inicio}/{fecha_fin}/"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=HEADERS)
            if r.status_code != 200:
                return []
            soup = BeautifulSoup(r.text, "html.parser")
            tabla = soup.find("table")
            if not tabla:
                return []
            fechas = []
