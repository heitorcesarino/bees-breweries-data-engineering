# tests/pipelines/test_gold_pipeline.py
import pandas as pd
from bees_breweries.pipelines.gold_pipeline import GoldBreweriesPipeline
from bees_breweries.storage.gold_storage import GoldStorage


def test_gold_pipeline_aggregates_breweries(tmp_path):
    silver_file = tmp_path / "silver.parquet"

    df = pd.DataFrame({
        "country": ["US", "US", "US"],
        "state": ["CA", "CA", "CA"],
        "city": ["LA", "LA", "SF"],
        "brewery_type": ["micro", "micro", "brewpub"],
    })
    df.to_parquet(silver_file)

    storage = GoldStorage(base_path=tmp_path)
    pipeline = GoldBreweriesPipeline(
        silver_path=str(silver_file),
        gold_storage=storage
    )

    output_path = pipeline.run()
    result = pd.read_parquet(output_path)

    assert len(result) == 2
    assert result.loc[result["city"] == "LA", "brewery_count"].iloc[0] == 2
