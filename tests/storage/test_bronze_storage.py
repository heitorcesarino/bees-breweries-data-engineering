import json
from datetime import date

from bees_breweries.storage.bronze_storage import BronzeStorage


def test_save_json_creates_partition_and_persists_data(tmp_path):
    """
    Should persist raw JSON data in the bronze layer,
    partitioned by ingestion_date.
    """
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

    # file was created
    assert file_path.exists()
    assert file_path.is_file()

    # partition path follows expected pattern
    expected_partition = (
        tmp_path
        / "breweries"
        / "ingestion_date=2025-02-20"
    )
    assert expected_partition.exists()
    assert expected_partition.is_dir()

    # data was persisted correctly
    with file_path.open(encoding="utf-8") as file:
        persisted_data = json.load(file)

    assert persisted_data == data
