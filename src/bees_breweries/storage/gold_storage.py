from pathlib import Path
import pandas as pd


class GoldStorage:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save(
        self,
        df: pd.DataFrame,
        filename: str = "breweries_by_type_and_location.parquet"
    ) -> Path:
        output_path = self.base_path / filename
        df.to_parquet(output_path, index=False)
        return output_path
