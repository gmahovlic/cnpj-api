from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    # Database
    database_path: str = "data/cnpj.db"

    # Auth — marketplace proxy secrets
    rapidapi_proxy_secret: str = ""
    zyla_proxy_secret: str = ""
    master_api_key: str = ""

    # Scheduler
    import_interval_hours: int = 24

    # Receita Federal
    receita_base_url: str = (
        "https://arquivos.receitafederal.gov.br/index.php/s/"
        "YggdBLfdninEJX9?dir=/{batch}"
    )


settings = Settings()
