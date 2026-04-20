from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "sqlite:///./ubid.db"
    t_high: float = 0.92          # auto-link threshold
    t_low: float = 0.65           # reject threshold (below → keep separate)
    observation_window_months: int = 12
    dormant_to_closed_months: int = 36
    consumption_active_threshold_kwh: float = 100.0

    class Config:
        env_file = ".env"


settings = Settings()
