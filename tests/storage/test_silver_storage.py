import pandas as pd

from bees_breweries.storage.silver_storage import SilverStorage


def test_save_breweries_creates_partitioned_parquet(tmp_path):
    """
    Should persist breweries data in the silver layer as partitioned parquet.
    """
    df = pd.DataFrame(
        [
            {
                "id": "1",
                "name": "Test Brewery",
                "brewery_type": "micro",
                "city": "Test City",
                "state": "Test State",
                "country": "Test Country",
            }
        ]
    )

    storage = SilverStorage(base_path=tmp_path)

    output_path = storage.save_breweries(df)

    # returns base directory
    assert output_path.exists()
    assert output_path.is_dir()

    # country partition exists (country=*)
    country_partitions = list(output_path.glob("country=*"))
    assert len(country_partitions) == 1

    # parquet file exists inside partition
    parquet_files = list(country_partitions[0].glob("*.parquet"))
    assert len(parquet_files) > 0

    # data can be read correctly
    persisted_df = pd.read_parquet(output_path)

    assert len(persisted_df) == 1
    assert persisted_df.loc[0, "id"] == "1"
    assert persisted_df.loc[0, "country"] == "Test Country"
