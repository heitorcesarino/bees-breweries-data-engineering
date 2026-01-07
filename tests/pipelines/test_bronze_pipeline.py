from unittest.mock import MagicMock

from bees_breweries.pipelines.bronze_pipeline import BronzeBreweriesPipeline


def test_bronze_pipeline_fetches_data_and_persists_to_storage():
    """
    Should fetch breweries from the client and persist them in the Bronze layer.
    """
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

    # Extraction
    mock_client.fetch_breweries.assert_called_once()

    # Persistence
    mock_storage.save_json.assert_called_once()

    _, kwargs = mock_storage.save_json.call_args

    assert kwargs["dataset"] == "breweries"
    assert kwargs["filename"] == "breweries.json"
    assert kwargs["data"] == mock_client.fetch_breweries.return_value
