from pathlib import Path
import pandas as pd

from bees_breweries.config.settings import settings
from bees_breweries.utils.logger import get_logger

logger = get_logger(__name__)


class SilverStorage:
    """
    Storage responsible for persisting cleaned and normalized data
    in the Silver layer.
    """
    def __init__(self, base_path: Path | None = None):
        """
        Initialize the Silver storage base path.
        """
        self.base_path = base_path or settings.SILVER_BREWERIES_PATH
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save_breweries(
        self,
        df: pd.DataFrame,
    ) -> Path:
        """
        Persist brewery data in Parquet format, partitioned by country.
        """
        logger.info(
            "Saving silver breweries data (partitioned)",
            extra={
                "records": len(df),
                "path": str(self.base_path),
                "partitions": ["country", "state", "city"],
            },
        )

        df.to_parquet(
            self.base_path,
            partition_cols=["country"],
            index=False,
        )

        return self.base_path
