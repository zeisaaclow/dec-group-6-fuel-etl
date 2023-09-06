from elt.connectors.fuel_api import FuelClient
from dotenv import load_dotenv
import os
import pytest

@pytest.fixture 
def setup():
    """
    Test fixture for setting up environment variables using dotenv.

    This fixture loads environment variables from a .env file using the dotenv library.
    It is intended to be used before running tests that require environment variables.

    """
    load_dotenv()



def test_fuel_client_get_fuel_api(setup):
    """
    Unit test for the FuelClient's get_fuel_api method.

    This test verifies the functionality of the get_fuel_api method of the FuelClient class.
    It checks if the method returns a dictionary with data and ensures that the dictionary
    is not empty.

    Dependencies:
        - The FuelClient module, which provides the FuelClient class.
        - Environment variables API_KEY and AUTHORIZATION should be set in a .env file.

    Args:
        setup (fixture): A pytest fixture that sets up environment variables.

    Test Steps:
        1. Load API_KEY and AUTHORIZATION from environment variables.
        2. Create a FuelClient instance with the API_KEY and AUTHORIZATION.
        3. Get an access token using the FuelClient.
        4. Retrieve fuel price data using the access token.

    Assertions:
        - Check that the fuel_price is a dictionary.
        - Ensure that the dictionary is not empty (contains data).

    """
    API_KEY_ID = os.environ.get("API_KEY")
    AUTHORIZATION = os.environ.get("AUTHORIZATION")
    fuel_client = FuelClient(api_key_id=API_KEY_ID, authorization=AUTHORIZATION)
    access_token = fuel_client.get_access_token()

    fuel_price = fuel_client.get_fuel_api(access_token, extract_type = "incremental")

    assert type(fuel_price) == dict
    assert len(fuel_price) > 0
