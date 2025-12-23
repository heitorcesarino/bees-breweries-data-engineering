from pathlib import Path
from pydantic import BaseModel


class Settings(BaseModel):
    # API
    BREWERY_API_BASE_URL: str = "https://api.openbrewerydb.org/v1/breweries"
    DEFAULT_PER_PAGE: int = 50
    REQUEST_TIMEOUT: int = 10

    # Base paths
    BRONZE_PATH: Path = Path("data/bronze")
    SILVER_PATH: Path = Path("data/silver")
    GOLD_PATH: Path = Path("data/gold")

    # Derived dataset paths (centralizados aqui)
    @property
    def BRONZE_BREWERIES_PATH(self) -> Path:
        return self.BRONZE_PATH / "breweries"

    @property
    def SILVER_BREWERIES_PATH(self) -> Path:
        return self.SILVER_PATH / "breweries"

    @property
    def GOLD_BREWERIES_PATH(self) -> Path:
        return self.GOLD_PATH / "breweries"


settings = Settings()
