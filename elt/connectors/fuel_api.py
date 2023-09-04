import requests 
import datetime

class FuelClient:
    
    def __init__(self, api_key_id: str, authorization: str):
        self.base_url = "https://api.onegov.nsw.gov.au"
        if api_key_id is None: 
            raise Exception("API key cannot be set to None.")
        self.api_key_id = api_key_id
        if authorization is None: 
            raise Exception("API secret key cannot be set to None.")
        self.authorization = authorization


    def get_access_token(self) -> dict:
        headers = {
            "grant_type" : "client_credentials",
            "Authorization": self.authorization,
        }
        response = requests.get(f"{self.base_url}/oauth/client_credential/accesstoken?grant_type=client_credentials", headers = headers)
        if response.status_code == 200: 
            return response.json()["access_token"]
        else: 
            raise Exception(f"Failed to extract data from Fuel API. Status Code: {response.status_code}. Response: {response.text}")

    def get_fuel_api(self, access_token, extract_type):

        current_time = datetime.datetime.now()
        request_timestamp = current_time.strftime('%d/%m/%Y')

        headers = {
        "Authorization": f"Bearer {access_token}",  
        "apikey" : self.api_key_id,
        "Content-Type": "application/json; charset=utf-8",
        "transactionid" : "1",
        "requesttimestamp" : request_timestamp
    }

        if extract_type == "full":
            request_url = "/FuelPriceCheck/v1/fuel/prices"
        elif extract_type == "incremental":
            request_url = "/FuelPriceCheck/v1/fuel/prices/new"
        response = requests.get(f"{self.base_url}{request_url}", headers = headers)
        if response.status_code == 200: 
            return response.json()
            # print(response.json())
        else: 
            raise Exception(f"Failed to extract data from Fuel API. Status Code: {response.status_code}. Response: {response.text}")
        