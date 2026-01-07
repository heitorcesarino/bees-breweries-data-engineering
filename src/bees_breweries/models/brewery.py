from pydantic import BaseModel
from typing import Optional


class Brewery(BaseModel):
    """
    Pydantic model representing a brewery record returned by the
    Open Brewery DB API.
    """
    id: str
    name: str
    brewery_type: str
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
