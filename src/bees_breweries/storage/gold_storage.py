from pathlib import Path
import pandas as pd

from bees_breweries.config.settings import settings
from bees_breweries.utils.logger import get_logger

logger = get_logger(__name__)


class GoldStorage:
    def __init__(self, base_path: str | None = None):
        # data/gold/breweries
        self.base_path = Path(base_path or settings.GOLD_PATH)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save_breweries(
        self,
        df: pd.DataFrame,
        filename: str = "breweries_by_type_and_location.parquet",
    ) -> Path:
        output_path = self.base_path / filename

        logger.info(
            "Saving gold breweries data",
            extra={
                "records": len(df),
                "path": str(output_path),
            },
        )

        df.to_parquet(output_path, index=False)
        return output_path
