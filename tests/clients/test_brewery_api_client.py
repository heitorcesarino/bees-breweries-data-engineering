import responses

from bees_breweries.clients.brewery import BreweryAPIClient


@responses.activate
def test_fetch_breweries_success():
    mock_response = [
        {
            "id": "1",
            "name": "Test Brewery",
            "brewery_type": "micro",
            "city": "Test City",
            "state": "Test State",
            "country": "Test Country",
        }
    ]

    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries",
        json=mock_response,
        status=200,
    )

    client = BreweryAPIClient()
    breweries = client.fetch_breweries(per_page=1)

    assert len(breweries) == 1
    assert breweries[0].name == "Test Brewery"
