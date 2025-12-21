from datetime import date
from typing import Optional

from bees_breweries.clients.brewery import BreweryAPIClient
from bees_breweries.storage.bronze_storage import BronzeStorage
from bees_breweries.utils.logger import get_logger

logger = get_logger(__name__)


class BronzeBreweriesPipeline:
    """
    Pipeline responsible for extracting breweries data from the API
    and persisting it in the Bronze layer as raw JSON.
    """

    def __init__(
        self,
        client: Optional[BreweryAPIClient] = None,
        storage: Optional[BronzeStorage] = None,
    ):
        self.client = client or BreweryAPIClient()
        self.storage = storage or BronzeStorage()

    def run(self, ingestion_date: Optional[date] = None) -> None:
        logger.info("Starting Bronze Breweries Pipeline")

        breweries = self.client.fetch_breweries()

        logger.info(
            "Fetched breweries from API",
            extra={"records": len(breweries)},
        )

        self.storage.save_json(
            dataset="breweries",
            data=breweries,
            ingestion_date=ingestion_date,
            filename="breweries.json",
        )

        logger.info("Bronze Breweries Pipeline finished successfully")
