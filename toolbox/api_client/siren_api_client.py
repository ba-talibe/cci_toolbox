import re
import os
import requests
from dotenv import load_dotenv
from typing import Optional, Dict, Any
from .api_client import APIClient


class SirenAPIClient(APIClient):
    """
    Client pour interagir avec une API REST.
    """

    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None, logger: Optional[Any] = None, siren_api_key: Optional[str] = None, cache_dir: Optional[str] = "siren_cache"):
        assert base_url, "L'URL de base ne peut pas être vide."
        
        super().__init__(base_url, headers, logger, cache_dir)
        self.siren_api_key = siren_api_key
        if siren_api_key:
            self.session.headers.update({'X-INSEE-Api-Key-Integration': siren_api_key})

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Envoie une requête GET à l'API.

        :param endpoint: Chemin relatif de l'endpoint (ex: '/users').
        :param params: Paramètres de requête facultatifs.
        :return: Données JSON en réponse ou None en cas d'erreur.
        """
        match = re.search(r"/(?:siren|siret)/(\d{9}|\d{14})", endpoint)
        if match:
            data_type = match.group(0)
            if data_type == "siren":
                data_type = data_type
            elif data_type == "siret":
                data_type = data_type[:9]
            cache_key = self._generate_cache_key(data_type, params)
            cached_data = self._read_cache(cache_key)

            if data_type == "siren":    
                return cached_data
            elif data_type == "siret":
                for etablissement in cached_data:
                    if etablissement.get("siret") == data_type:
                        return etablissement

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if data_type == "siren":
                self._write_cache(cache_key, data)
            return data
        except requests.exceptions.HTTPError as e:
            print(f"[HTTP ERROR] {e} - Status: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[REQUEST FAILED] {e}")
        except ValueError:
            print("[ERROR] La réponse n'est pas au format JSON valide.")
        return None

    
    def get_with_q_parameter(self, endpoint: str,  params: Optional[Dict[str, Any]] = None, historized=False) -> Optional[Dict[str, Any]]:
        """
        Envoie une requête GET à l'API avec un paramètre de recherche 'q'.
        """
        if params is None:
            self.get(endpoint)

        for key, value in params.items():
            sub_query = f"{key}:{value}"

        if historized:
            params = {'q':f"periode({sub_query})"}
        else:
            params = {'q': sub_query}

        return self.get(endpoint, params=params)



    def get_data_by_siren(self, siren: str, params : Optional[Dict[str, Any]]=None,) -> Optional[Dict[str, Any]]:
        """
        Récupère les données d'un etablissement pour un numéro SIREN donné.
        """
        endpoint = f"/siren/{siren}"
        data = self.get(endpoint)
        if data:
            return data
        else:
            print(f"Erreur lors de la récupération des données pour le SIREN {siren}.")
            return None

    def get_data_by_siret(self, siret: str, params : Optional[Dict[str, Any]]=None) -> Optional[Dict[str, Any]]:
        """
        Récupère les données SIRET pour un numéro SIRET donné.
        """
        endpoint = f"/siret/{siret}"
        data = self.get(endpoint)
        if data:
            return data
        else:
            print(f"Erreur lors de la récupération des données pour le SIRET {siret}.")
            return None

    def get_data_by_q_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None, historized=False) -> Optional[Dict[str, Any]]:
        """
        Récupère les données de l'API en utilisant un paramètre de recherche 'q'.
        """
        data = self.get_with_q_parameter(endpoint, params=params, historized=historized)
        if data:
            return data
        else:
            print(f"Erreur lors de la récupération des données pour l'endpoint {endpoint}.")
            return None
