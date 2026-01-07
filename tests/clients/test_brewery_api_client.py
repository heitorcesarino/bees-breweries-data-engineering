import responses

from bees_breweries.clients.brewery import BreweryAPIClient
from bees_breweries.models.brewery import Brewery


@responses.activate
def test_fetch_breweries_handles_pagination():
    first_page = [
        {
            "id": "1",
            "name": "Brewery Page 1",
            "brewery_type": "micro",
            "city": "City 1",
            "state": "State 1",
            "country": "Country 1",
        }
    ]

    # Page 1 -> returns data
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries",
        json=first_page,
        status=200,
        match=[responses.matchers.query_param_matcher({"page": "1", "per_page": "1"})],
    )

    # Page 2 -> empty list (end of pagination)
    responses.add(
        responses.GET,
        "https://api.openbrewerydb.org/v1/breweries",
        json=[],
        status=200,
        match=[responses.matchers.query_param_matcher({"page": "2", "per_page": "1"})],
    )

    client = BreweryAPIClient()
    result = client.fetch_breweries(per_page=1)

    assert len(result) == 1
    assert isinstance(result[0], Brewery)
