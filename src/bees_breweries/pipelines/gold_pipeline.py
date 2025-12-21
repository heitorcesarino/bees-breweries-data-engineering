import pandas as pd
from bees_breweries.storage.gold_storage import GoldStorage


class GoldBreweriesPipeline:
    def __init__(self, silver_path: str, gold_storage: GoldStorage):
        self.silver_path = silver_path
        self.gold_storage = gold_storage

    def run(self):
        df = pd.read_parquet(self.silver_path)

        aggregated_df = (
            df
            .groupby(["country", "state", "city", "brewery_type"])
            .size()
            .reset_index(name="brewery_count")
        )

        return self.gold_storage.save(aggregated_df)
