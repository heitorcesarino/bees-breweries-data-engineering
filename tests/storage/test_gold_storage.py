# tests/storage/test_gold_storage.py
import pandas as pd
from bees_breweries.storage.gold_storage import GoldStorage


def test_gold_storage_save(tmp_path):
    df = pd.DataFrame({
        "country": ["US"],
        "state": ["CA"],
        "city": ["LA"],
        "brewery_type": ["micro"],
        "brewery_count": [10],
    })

    storage = GoldStorage(base_path=tmp_path)
    output_path = storage.save(df)

    assert output_path.exists()
