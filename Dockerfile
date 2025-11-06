# Etapa 1: build
FROM python:3.12-slim AS builder

# Configura diretório de trabalho
WORKDIR /app

# Instala dependências de sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements primeiro (melhor cache)
COPY requirements.txt .

# Instala dependências no builder
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Etapa 2: imagem final
FROM python:3.12-slim

WORKDIR /app

# Copia dependências do builder
COPY --from=builder /usr/local /usr/local

# Copia o código
COPY app ./app
COPY .env ./

# Porta padrão do FastAPI
EXPOSE 8000

# Variável opcional para GCP credentials
ENV GCP_SERVICE_ACCOUNT_KEY=/app/bigquery-loader@gobrax-data.iam.gserviceaccount.com.json

# Comando padrão do container
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
