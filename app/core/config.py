from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    # Variáveis necessárias para o microserviço
    GCP_PROJECT: str = Field(..., description="ID do projeto GCP")
    GOOGLE_APPLICATION_CREDENTIALS_JSON: str = Field(..., description="Credenciais da conta de serviço GCP em formato JSON")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # <--- ignora variáveis não declaradas (como as do Airflow)
    )

settings = Settings()
