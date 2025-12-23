from bees_breweries.pipelines.bronze_pipeline import BronzeBreweriesPipeline
from bees_breweries.pipelines.silver_pipeline import SilverBreweriesPipeline
from bees_breweries.pipelines.gold_pipeline import GoldBreweriesPipeline
from bees_breweries.utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    logger.info("Starting full Breweries pipeline")

    bronze_pipeline = BronzeBreweriesPipeline()
    bronze_pipeline.run()

    silver_pipeline = SilverBreweriesPipeline()
    silver_pipeline.run()

    gold_pipeline = GoldBreweriesPipeline()
    gold_pipeline.run()

    logger.info("Breweries pipeline finished successfully")


if __name__ == "__main__":
    main()
