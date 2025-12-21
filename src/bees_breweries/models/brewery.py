from pydantic import BaseModel
from typing import Optional


class Brewery(BaseModel):
    id: str
    name: str
    brewery_type: str
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
