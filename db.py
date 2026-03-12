import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ ERROR: DATABASE_URL no configurada en las variables de entorno.")

# ✅ Convertir a driver async si es necesario
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# ✅ Limpieza de parámetros para compatibilidad con Neon
if "?" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.split("?")[0] + "?ssl=require"
else:
    DATABASE_URL += "?ssl=require"

# ✅ Configuración del Motor (Optimizado para Neon free tier)
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,      # reconecta si la DB se "durmió"
    pool_size=5,
    max_overflow=10,
    pool_recycle=300,        # 5 min — Neon cierra conexiones inactivas rápido
    connect_args={
        "command_timeout": 60
    }
)

# ── Session para FastAPI (inyección de dependencia) ──
SessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# ── Session para tareas en background (aprender, retroactivo) ──
# Requerido por main.py para _run_aprender y _run_retroactivo
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def get_db():
    async with SessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
