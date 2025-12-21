from unittest.mock import MagicMock

from bees_breweries.pipelines.bronze_pipeline import BronzeBreweriesPipeline


def test_bronze_pipeline_runs_extraction_and_persistence():
    mock_client = MagicMock()
    mock_storage = MagicMock()

    mock_client.fetch_breweries.return_value = [
        {"id": "1", "name": "Test Brewery"}
    ]

    pipeline = BronzeBreweriesPipeline(
        client=mock_client,
        storage=mock_storage,
    )

    pipeline.run()

    mock_client.fetch_breweries.assert_called_once()
    mock_storage.save_json.assert_called_once()

    args, kwargs = mock_storage.save_json.call_args
    assert kwargs["dataset"] == "breweries"
    assert kwargs["filename"] == "breweries.json"
