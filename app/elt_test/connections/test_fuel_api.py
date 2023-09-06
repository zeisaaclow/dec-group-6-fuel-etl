from elt.connectors.fuel_api import FuelClient
from dotenv import load_dotenv
import os
import pytest

@pytest.fixture 
def setup():
    load_dotenv()



def test_fuel_client_get_fuel_api(setup):
    API_KEY_ID = os.environ.get("API_KEY")
    AUTHORIZATION = os.environ.get("AUTHORIZATION")
    fuel_client = FuelClient(api_key_id=API_KEY_ID, authorization=AUTHORIZATION)
    access_token = fuel_client.get_access_token()

    fuel_price = fuel_client.get_fuel_api(access_token, extract_type = "incremental")

    assert type(fuel_price) == dict
    assert len(fuel_price) > 0