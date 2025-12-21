from pathlib import Path
import pandas as pd

from bees_breweries.utils.logger import get_logger

logger = get_logger(__name__)


class SilverStorage:
    def __init__(self, base_path: str = "data/silver"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save_breweries(self, df: pd.DataFrame) -> Path:
        output_path = self.base_path / "breweries"
        output_path.mkdir(parents=True, exist_ok=True)

        file_path = output_path / "breweries.parquet"

        logger.info("Saving silver breweries data to %s", file_path)
        df.to_parquet(file_path, index=False)

        return file_path
