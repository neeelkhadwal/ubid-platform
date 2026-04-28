from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "sqlite:///./ubid.db"
    t_high: float = 0.92          # auto-link threshold
    t_low: float = 0.65           # reject threshold (below → keep separate)
    observation_window_months: int = 12
    dormant_to_closed_months: int = 36
    consumption_active_threshold_kwh: float = 100.0

    # API authentication: JSON map of api_key → {"name": str, "role": viewer|reviewer|admin}
    # Empty by default — endpoints will return 503 until configured.
    api_keys: dict = {}

    # CORS allowlist. Wildcard is rejected — set explicit origins.
    cors_origins: list[str] = [
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ]

    class Config:
        env_file = ".env"


settings = Settings()
