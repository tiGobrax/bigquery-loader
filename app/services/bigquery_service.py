import os
import json
import time
import pandas as pd
import polars as pl
from google.cloud import bigquery
from loguru import logger


class BigQueryService:
    """Serviço responsável por carregar dados no BigQuery a partir de JSON ou Parquet."""

    def __init__(self, project_id: str, dataset_id: str):
        self.project = project_id
        self.dataset = dataset_id

        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        service_account_secret = os.getenv("GCP_SERVICE_ACCOUNT_KEY")

        # --- Lógica de autenticação ---
        if credentials_path and os.path.exists(credentials_path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
            logger.info(f"Usando credenciais locais: {credentials_path}")

        elif service_account_secret:
            temp_path = "/tmp/gcp_service_account.json"
            try:
                key_data = json.loads(service_account_secret)
                with open(temp_path, "w") as f:
                    json.dump(key_data, f)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_path
                logger.info("Credenciais carregadas a partir do secret GCP_SERVICE_ACCOUNT_KEY")
            except Exception as e:
                logger.error(f"Erro ao processar secret GCP_SERVICE_ACCOUNT_KEY: {e}")
                raise
        else:
            raise RuntimeError(
                "Nenhuma credencial GCP encontrada. Defina GOOGLE_APPLICATION_CREDENTIALS ou GCP_SERVICE_ACCOUNT_KEY."
            )

        # --- Inicializa cliente BigQuery ---
        self.client = bigquery.Client(project=self.project)
        logger.info(f"Conectado ao BigQuery: {self.project}.{self.dataset}")

    # -------------------------
    # Carga a partir de JSON
    # -------------------------

    def load_table_from_json(self, table_name: str, data: list):
        start_time = time.time()
        df = pd.DataFrame(data)
        if df.empty:
            logger.warning(f"Nenhum dado para carregar em {table_name}.")
            return {"status": "warning", "message": f"Nenhum dado para carregar em {table_name}"}

        return self._load_to_bigquery(df, table_name, start_time)

    # -------------------------
    # Carga a partir de Parquet
    # -------------------------

    def load_table_from_parquet(self, table_name: str, file_path: str):
        start_time = time.time()
        try:
            pl_df = pl.read_parquet(file_path)
            df = pl_df.to_pandas()
        except Exception as e:
            logger.error(f"Erro ao ler arquivo Parquet: {e}")
            raise

        if df.empty:
            logger.warning(f"Arquivo Parquet vazio: {file_path}")
            return {"status": "warning", "message": f"Arquivo Parquet vazio: {file_path}"}

        return self._load_to_bigquery(df, table_name, start_time)

    # -------------------------
    # Auxiliar interno
    # -------------------------

    def _load_to_bigquery(self, df: pd.DataFrame, table_name: str, start_time: float):
        table_id = f"{self.project}.{self.dataset}.{table_name}"
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

        logger.info(f"Iniciando carga para {table_id} ({len(df)} linhas)")
        try:
            load_job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            load_job.result()
            elapsed = time.time() - start_time

            msg = (
                f"Carga concluída: {table_id} ({len(df)} registros, "
                f"{elapsed:.1f}s = {elapsed/60:.2f} min)"
            )
            logger.success(msg)
            return {"status": "success", "message": msg}

        except Exception as e:
            logger.error(f"Erro ao carregar dados para {table_id}: {e}")
            raise
