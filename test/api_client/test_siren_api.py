import os
import pytest
from unittest.mock import patch, MagicMock
from toolbox.api_client.siren_api_client import SirenAPIClient  # à adapter selon ton projet
from requests.exceptions import HTTPError
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

@pytest.fixture
def client():
    siren_api_key = os.getenv("siren_api_key")
    siren_api_url = os.getenv("siren_api_url")
    return SirenAPIClient(siren_api_url, siren_api_key=siren_api_key)

# Test du constructeur
def test_init_headers_includes_api_key(client):
    assert client.session.headers.get("X-INSEE-Api-Key-Integration") == os.getenv("siren_api_key")

# Test de base du GET - succès
def test_get_success(client):
    mock_json = {"data": "test"}
    with patch("requests.Session.get", return_value=MagicMock(status_code=200, json=lambda: mock_json)) as mock_get:
        response = client.get("/siren/123456789")
        assert response == mock_json
        mock_get.assert_called_once()

# Test GET - échec HTTP
def test_get_http_error(client):
    response_mock = MagicMock()
    response_mock.raise_for_status.side_effect = HTTPError("Bad Request")
    response_mock.status_code = 400
    with patch("requests.Session.get", return_value=response_mock):
        result = client.get("/test")
        assert result is None

# Test GET - échec JSON
def test_get_json_decode_error(client):
    bad_response = MagicMock()
    bad_response.raise_for_status.return_value = None
    bad_response.json.side_effect = ValueError("Invalid JSON")
    with patch("requests.Session.get", return_value=bad_response):
        result = client.get("/test")
        assert result is None

def test_get_cache_key(client):
    # Test avec un endpoint SIREN
    endpoint = "/siren/123456789"
    params = {"param1": "value1", "param2": "value2"}
    expected_key = "siren__param1=value1_param2=value2"
    assert client._generate_cache_key(endpoint, params) == expected_key

    # Test avec un endpoint SIRET
    endpoint = "/siret/12345678901234"
    params = {"param1": "value1", "param2": "value2"}
    expected_key = "siret__param1=value1_param2=value2"
    assert client._generate_cache_key(endpoint, params) == expected_key