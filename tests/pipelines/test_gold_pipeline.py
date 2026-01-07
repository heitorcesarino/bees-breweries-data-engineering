import pandas as pd
from pathlib import Path

from bees_breweries.pipelines.gold_pipeline import GoldBreweriesPipeline
from bees_breweries.storage.gold_storage import GoldStorage


def test_gold_pipeline_aggregates_breweries(tmp_path: Path):
    """
    Should aggregate brewery counts by location and brewery type
    from the silver layer and persist the result in the gold layer.
    """
    # Arrange: create a silver parquet file with sample data
    silver_file = tmp_path / "silver.parquet"

    df = pd.DataFrame(
        {
            "country": ["US", "US", "US"],
            "state": ["CA", "CA", "CA"],
            "city": ["LA", "LA", "SF"],
            "brewery_type": ["micro", "micro", "brewpub"],
        }
    )

    df.to_parquet(silver_file)

    storage = GoldStorage(base_path=tmp_path)

    pipeline = GoldBreweriesPipeline(
        silver_path=silver_file,
        storage=storage,
    )

    # Act: run gold pipeline
    output_path = pipeline.run()
    result = pd.read_parquet(output_path)

    # Assert: two aggregated rows (LA/micro and SF/brewpub)
    assert len(result) == 2

    # Assert: LA has two micro breweries
    la_count = result.loc[result["city"] == "LA", "brewery_count"].iloc[0]
    assert la_count == 2
