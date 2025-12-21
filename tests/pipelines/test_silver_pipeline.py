import pandas as pd

from bees_breweries.storage.silver_storage import SilverStorage


def test_save_breweries(tmp_path):
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
    file_path = storage.save_breweries(df)

    assert file_path.exists()
