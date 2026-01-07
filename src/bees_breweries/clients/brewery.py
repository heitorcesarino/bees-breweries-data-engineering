import requests
from typing import List

from bees_breweries.config.settings import settings
from bees_breweries.models.brewery import Brewery


class BreweryAPIClient:
    """
    Client responsible for consuming the Open Brewery DB public API.

    This class encapsulates all HTTP communication with the external
    Open Brewery DB service, abstracting request configuration,
    pagination handling and response parsing.

    It acts as the ingestion entry point of the project, being used
    by upstream pipelines (bronze/silver) to retrieve raw brewery data
    in a structured and typed format.
    """

    def __init__(self) -> None:
        """
        Initialize the API client with configuration parameters.

        The base URL and request timeout are loaded from the centralized
        application settings, ensuring consistency and easy configuration
        across environments.
        """
        self.base_url = settings.BREWERY_API_BASE_URL
        self.timeout = settings.REQUEST_TIMEOUT

    def fetch_breweries(self, per_page: int = settings.DEFAULT_PER_PAGE) -> List[Brewery]:
        """
        Fetch all breweries from the Open Brewery DB API using pagination.

        This method iteratively requests all available pages from the API
        until no more records are returned. Each API response item is
        converted into a strongly-typed `Brewery` domain model.

        Pagination is controlled via the `page` and `per_page` query
        parameters, following the API contract.

        Args:
            per_page (int): Number of records to request per API page.
                Defaults to the value defined in application settings.

        Returns:
            List[Brewery]: A list of Brewery objects representing all
            breweries returned by the API.

        Raises:
            requests.HTTPError: If the API responds with a non-success
            HTTP status code.
            requests.RequestException: For network-related errors,
            timeouts or request failures.

        Example:
            >>> client = BreweryAPIClient()
            >>> breweries = client.fetch_breweries(per_page=50)
            >>> len(breweries)
            8000
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
