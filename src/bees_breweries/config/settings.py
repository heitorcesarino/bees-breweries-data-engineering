from pydantic import BaseModel


class Settings(BaseModel):
    BREWERY_API_BASE_URL: str = "https://api.openbrewerydb.org/v1/breweries"
    DEFAULT_PER_PAGE: int = 50
    REQUEST_TIMEOUT: int = 10
    BRONZE_PATH: str = "data/bronze"
    SILVER_PATH: str = "data/silver"
    GOLD_PATH: str = "data/gold"


settings = Settings()
