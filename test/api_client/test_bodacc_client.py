import os
import json
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from toolbox.api_client.bodacc_api_client import BodaccAPIClient



CACHE_DIR = "test_bodacc_cache"

@pytest.fixture
def client():
    client = BodaccAPIClient(base_url="https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/records")
    client._get_cache_filepath = lambda date: os.path.join(CACHE_DIR, f"{date}.json")
    return client

@pytest.fixture(autouse=True)
def setup_and_teardown():
    os.makedirs(CACHE_DIR, exist_ok=True)
    yield
    for f in os.listdir(CACHE_DIR):
        os.remove(os.path.join(CACHE_DIR, f))
    os.rmdir(CACHE_DIR)

# _write_cache / _read_cache
def test_cache_write_and_read(client):
    date = "2025-01-01"
    data = [{"id": 1, "commercant": "Test"}]
    client._write_cache(date, data)
    assert os.path.exists(client._get_cache_filepath(date))
    loaded = client._read_cache(date)
    assert loaded == data

# fetch_data_for_date utilise cache
def test_fetch_data_for_date_uses_cache(client):
    date = "2025-01-02"
    data = [{"id": 2, "commercant": "Cached"}]
    client._write_cache(date, data)
    with patch.object(client, 'fetch_all_data_from_api') as mock_api:
        result = client.fetch_data_for_date(date)
        assert result == data
        mock_api.assert_not_called()

# fetch_data_for_date appelle API si pas de cache
def test_fetch_data_for_date_calls_api(client):
    date = "2025-01-03"
    api_data = [{
        "id": "abc",
        "registre": ["123456789", "123 456 789"],
        "dateparution": date,
        "numerodepartement": "75",
        "commercant": "API",
        "jugement": {"type": "initial"},
        "numeroannonce": "1001"
    }]
    with patch.object(client, 'fetch_all_data_from_api', return_value=api_data):
        result = client.fetch_data_for_date(date)
        assert result[0]["SIREN"] == "123456789"
        assert result[0]["commercant"] == "API"
