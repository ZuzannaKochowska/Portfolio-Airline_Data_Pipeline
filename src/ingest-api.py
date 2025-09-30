import os
import json
import requests
import pandas as pd
from datetime import datetime

print("Current working directory:", os.getcwd())

with open("C:/Users/Zuza/PycharmProjects/Portfolio-Airline_Data_Pipeline/credentials.json") as f:
    creds = json.load(f)
    CLIENT_ID = creds["clientId"]
    CLIENT_SECRET = creds["clientSecret"]

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
API_URL = "https://opensky-network.org/api/states/all"


def get_access_token(client_id: str, client_secret: str) -> str:
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    resp = requests.post(TOKEN_URL, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_flight_data(token: str) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(API_URL, headers=headers)
    resp.raise_for_status()
    return resp.json()


def save_raw_data(data: dict):
    # folder docelowy: raw/flights/YYYY-MM-DD/
    today = datetime.utcnow().strftime("%Y-%m-%d")
    raw_path = f"raw/flights/{today}/"
    os.makedirs(raw_path, exist_ok=True)

    json_path = os.path.join(raw_path, "flights.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    states = data.get("states", [])
    if not states:
        print("⚠️ Brak danych z API!")
        return

    columns = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
        "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
        "spi", "position_source"
    ]


    df = pd.DataFrame(states, columns=columns)

    parquet_path = os.path.join(raw_path, "flights.parquet")
    df.to_parquet(parquet_path, index=False)

    print(f"✅ Dane zapisane w {raw_path}")


if __name__ == "__main__":
    try:
        token = get_access_token(CLIENT_ID, CLIENT_SECRET)
        data = fetch_flight_data(token)
        save_raw_data(data)
    except Exception as e:
        print(f"❌ Błąd podczas pobierania danych: {e}")
