from dotenv import load_dotenv
load_dotenv()  # <<< ADICIONE ESTA LINHA ANTES DE QUALQUER OUTRA IMPORTAÇÃO

from fastapi import FastAPI
from loguru import logger
from app.routers.bigquery_router import router as bigquery_router

# Inicializa aplicação FastAPI
app = FastAPI(
    title="BigQuery Loader Service",
    version="1.0.0",
    description="Microserviço responsável por cargas e operações no BigQuery para pipelines da Gobrax",
)

# Configura log básico
logger.add("logs/bigquery_loader.log", rotation="10 MB", retention="7 days", level="INFO", enqueue=True)
logger.info("🚀 BigQuery Loader iniciado")

# Registra rotas
app.include_router(bigquery_router, prefix="/api/v1/bigquery", tags=["BigQuery"])

# Health check simples
@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "bigquery-loader"}
