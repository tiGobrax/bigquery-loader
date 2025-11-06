import os
import json
import time
import pandas as pd
import polars as pl
from google.cloud import bigquery
from loguru import logger


class BigQueryService:
    """Serviço responsável por carregar dados no BigQuery a partir de JSON ou Parquet."""

    def __init__(self):
        credentials_path = os.getenv("GCP_SERVICE_ACCOUNT_KEY")

        if not credentials_path:
            raise RuntimeError("Variável GCP_SERVICE_ACCOUNT_KEY não definida.")

        if os.path.exists(credentials_path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
            logger.info(f"Usando credenciais locais: {credentials_path}")
        else:
            try:
                key_data = json.loads(credentials_path)
                temp_path = "/tmp/gcp_service_account.json"
                with open(temp_path, "w") as f:
                    json.dump(key_data, f)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_path
                logger.info("Credenciais carregadas a partir de JSON embutido em GCP_SERVICE_ACCOUNT_KEY")
            except json.JSONDecodeError:
                raise RuntimeError(f"O caminho {credentials_path} não existe e o conteúdo não é JSON válido.")

        self.client = bigquery.Client()
        logger.info("Cliente BigQuery inicializado com sucesso.")

    # -------------------------
    # Carga a partir de JSON
    # -------------------------

    def load_table_from_json(self, project_id: str, dataset_id: str, table_name: str, data: list):
        start_time = time.time()
        df = pd.DataFrame(data)
        if df.empty:
            logger.warning(f"Nenhum dado para carregar em {table_name}.")
            return {"status": "warning", "message": f"Nenhum dado para carregar em {table_name}"}

        return self._load_to_bigquery(project_id, dataset_id, table_name, df, start_time)

    # -------------------------
    # Carga a partir de Parquet
    # -------------------------

    def load_table_from_parquet(self, project_id: str, dataset_id: str, table_name: str, file_path: str):
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

        return self._load_to_bigquery(project_id, dataset_id, table_name, df, start_time)

    # -------------------------
    # Auxiliar interno
    # -------------------------

    def _load_to_bigquery(self, project_id: str, dataset_id: str, table_name: str, df: pd.DataFrame, start_time: float):
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

        logger.info(f"Iniciando carga para {table_id} ({len(df)} linhas)")
        try:
            load_job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            load_job.result()
            elapsed = time.time() - start_time

            msg = f"Carga concluída: {table_id} ({len(df)} registros, {elapsed:.1f}s)"
            logger.success(msg)
            return {"status": "success", "message": msg}

        except Exception as e:
            logger.error(f"Erro ao carregar dados para {table_id}: {e}")
            raise
