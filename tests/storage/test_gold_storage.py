import pandas as pd

from bees_breweries.storage.gold_storage import GoldStorage


def test_save_breweries_creates_parquet_file(tmp_path):
    """
    Should persist aggregated breweries data in the gold layer as parquet.
    """
    df = pd.DataFrame(
        {
            "country": ["US"],
            "state": ["CA"],
            "city": ["LA"],
            "brewery_type": ["micro"],
            "brewery_count": [10],
        }
    )

    storage = GoldStorage(base_path=tmp_path)

    output_path = storage.save_breweries(df)

    # file was created
    assert output_path.exists()
    assert output_path.is_file()
    assert output_path.suffix == ".parquet"

    # data can be read correctly
    persisted_df = pd.read_parquet(output_path)

    assert len(persisted_df) == 1
    assert persisted_df.loc[0, "brewery_count"] == 10
