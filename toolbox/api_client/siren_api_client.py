import os
from typing import Optional, Dict, Any
from dotenv import load_dotenv
import requests



class SirenAPIClient:
    """
    Client pour interagir avec une API REST.
    """

    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None, siren_api_key: Optional[str] = None):
        assert base_url, "L'URL de base ne peut pas être vide."
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        default_headers =  {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        if headers:
            default_headers.update(headers)
        self.session.headers.update(default_headers)
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
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            # print(response.url)
            return response.json()
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


if __name__ == "__main__":
    # Exemple d'utilisation
    siren = "889932059"
    siret = "88993205900013"

    os.environ["siren_api_key"] = "34989d4c-5795-469b-989d-4c5795469b79"
    os.environ["siren_api_url"] = "https://api.insee.fr/api-sirene/3.11/"

    siren_api_key = os.getenv("siren_api_key")
    siren_api_url = os.getenv("siren_api_url")


    siren_api_client = SirenAPIClient(siren_api_url, siren_api_key=siren_api_key)

    siren_data = siren_api_client.get_data_by_siren(siren)
    siret_data = siren_api_client.get_data_by_siret(siret)

    print(siren_data)
    print(siret_data)




    