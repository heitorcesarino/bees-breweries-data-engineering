import requests
from typing import List

from bees_breweries.config.settings import settings
from bees_breweries.models.brewery import Brewery


class BreweryAPIClient:
    """
    Client responsible for consuming the Open Brewery DB API.
    """

    def __init__(self) -> None:
        self.base_url = settings.BREWERY_API_BASE_URL
        self.timeout = settings.REQUEST_TIMEOUT

    def fetch_breweries(self, per_page: int = settings.DEFAULT_PER_PAGE) -> List[Brewery]:
        """
        Fetch all breweries from the API using pagination.
        """
        page = 1
        breweries: List[Brewery] = []

        while True:
            response = requests.get(
                self.base_url,
                params={"per_page": per_page, "page": page},
                timeout=self.timeout,
            )

            response.raise_for_status()
            data = response.json()

            if not data:
                break

            breweries.extend(Brewery(**item) for item in data)
            page += 1

        return breweries
