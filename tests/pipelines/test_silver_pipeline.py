import pandas as pd

from bees_breweries.storage.silver_storage import SilverStorage


def test_save_breweries_creates_partitioned_parquet(tmp_path):
    df = pd.DataFrame(
        [
            {
                "id": "1",
                "name": "Test Brewery",
                "brewery_type": "Micro Brewery",
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

    # there is at least one country partition (country=*)
    country_partitions = list(output_path.glob("country=*"))
    assert len(country_partitions) == 1

    country_partition = country_partitions[0]
    assert country_partition.is_dir()

    # parquet files exist inside the partition
    parquet_files = list(country_partition.glob("*.parquet"))
    assert len(parquet_files) > 0

    # data can be read correctly
    result = pd.read_parquet(output_path)

    assert len(result) == 1
    assert result.loc[0, "name"] == "Test Brewery"
    assert result.loc[0, "country"] == "Test Country"
