from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Configurações globais do serviço BigQuery Loader."""
    gcp_service_account_key: str = Field(..., env="GCP_SERVICE_ACCOUNT_KEY")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
