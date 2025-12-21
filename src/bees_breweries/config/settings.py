from pydantic import BaseModel


class Settings(BaseModel):
    BREWERY_API_BASE_URL: str = "https://api.openbrewerydb.org/v1/breweries"
    DEFAULT_PER_PAGE: int = 50
    REQUEST_TIMEOUT: int = 10


settings = Settings()
