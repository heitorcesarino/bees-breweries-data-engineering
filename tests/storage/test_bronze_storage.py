import json
from datetime import date

from bees_breweries.storage.bronze_storage import BronzeStorage


def test_save_json_creates_file_and_persists_data(tmp_path):
    storage = BronzeStorage(base_path=tmp_path)

    data = [
        {"id": "1", "name": "Test Brewery"},
        {"id": "2", "name": "Another Brewery"},
    ]

    ingestion_date = date(2025, 2, 20)

    file_path = storage.save_json(
        dataset="breweries",
        data=data,
        ingestion_date=ingestion_date,
        filename="breweries.json",
    )

    assert file_path.exists()

    with file_path.open() as file:
        persisted_data = json.load(file)

    assert persisted_data == data
