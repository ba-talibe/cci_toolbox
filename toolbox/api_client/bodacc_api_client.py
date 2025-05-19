import os
import json
import requests
import pandas as pd
from .api_client import APIClient
from datetime import datetime
from urllib.parse import urlencode
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed




class BodaccAPIClient(APIClient):
    """
    Client pour interagir avec une API REST.
    """

    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None, logger: Optional[Any] = None, cache_dir: Optional[str] = "bodacc_cache"):
        """
        Initialise le client API avec l'URL de base et les en-têtes par défaut. 
        assert base_url, "L'URL de base ne peut pas être vide."
        :param base_url: URL de base de l'API.
        :param headers: En-têtes par défaut à utiliser pour les requêtes.
        :param logger: Logger pour les messages d'information et d'erreur.
        """
        super().__init__(base_url, headers, logger, cache_dir)



    def fetch_chunck(self, query_list,  offset, limit, headers=None):
        try:
            full_query = query_list + [('limit', str(limit)), ('offset', str(offset))]
            full_url = f"{self.base_url}?{urlencode(full_query, doseq=True)}"

            response = requests.get(full_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            
            if self.logger:
                self.logger.info(f"✅ Page {offset // limit + 1} : {len(results)} éléments récupérés.")
            
            return results
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Erreur offset {offset} : {e}")
            return []

    def fetch_all_data_from_api(self,  query_list=None, headers=None, max_workers=5):
        """
        Récupère toutes les données depuis une API paginée (100 max par requête) en parallèle.
        """
        query_list = query_list or []
        limit = 100

        # Étape 1 : Obtenir le total_count
        first_query = query_list + [('limit', str(limit)), ('offset', '0')]
        first_url = f"{self.base_url}?{urlencode(first_query, doseq=True)}"
        if headers:
            self.session.headers.update(headers)
        response = requests.get(first_url, headers=self.session.headers)
        response.raise_for_status()
        data = response.json()

        total_count = data.get("total_count", 0)
        first_results = data.get("results", [])

        if self.logger:
            self.logger.info(f"Total de résultats à récupérer : {total_count}")

        # Étape 2 : Générer tous les offsets nécessaires
        offsets = list(range(limit, total_count, limit))  # on a déjà fait l'offset 0

        all_results = first_results.copy()

        # Étape 3 : Télécharger les pages suivantes en parallèle
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.fetch_chunck, query_list, offset, limit, headers)
                for offset in offsets
            ]
            for future in as_completed(futures):
                results = future.result()
                all_results.extend(results)

        return all_results

    
    def fetch_data_for_date(self, date, queries=None):
        """
        Appelle l'API pour une date donnée et structure les résultats.

        :param date: Date au format 'YYYY-MM-DD'.
        :param headers: En-têtes à utiliser pour la requête.
        :param queries: Liste de tuples de requêtes supplémentaires.
        :return: Liste de dictionnaires contenant les données récupérées.
        """

        # 1. Lire depuis le cache
        cached_data = self._read_cache(date)
        if cached_data is not None:
            if self.logger:
                self.logger.info(f"Cache utilisé pour la date {date}")
            return cached_data
    
        query_list = [
                    ('refine', 'familleavis_lib:"Procédures collectives"'),
                    ('refine', f"dateparution:{date}"),
                ]


        if queries:
            query_list.extend(queries)

        try:
            fetched_data = self.fetch_all_data_from_api(
                query_list=query_list
            )

            processed_data = [
                {
                    "id": row.get("id"),
                    "registre": row.get("registre"),
                    "dateparution": row.get("dateparution"),
                    "numerodepartement": row.get("numerodepartement"),
                    "commercant": row.get("commercant"),
                    "jugement": row.get("jugement"),
                    "numeroannonce": row.get("numeroannonce"),
                    "SIREN": row.get("registre")[0] if row.get("registre") else None,
                }
                for row in fetched_data
            ]

            self._write_cache(date, processed_data)
            return processed_data
        except Exception as e:
            print(f"❌ Erreur pour la date {date}: {e}")
            return []


    def fetch_and_clean_api_data(self, start_date, queries=None, max_workers=5):
        """
        Récupère et nettoie les données de l'API pour un département donné.
        """
        interesting_columns = ["id", "registre", "dateparution", "numerodepartement", "commercant", "jugement", "numeroannonce", "SIREN"]



        # 2. Créer une plage de dates journalières entre D et aujourd'hui
        date_range = pd.date_range(start=start_date, end=datetime.today(), freq='D')
        date_range = date_range.strftime("%Y-%m-%d").tolist()  
    

        new_data = pd.DataFrame(columns=interesting_columns)
        data = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(self.fetch_data_for_date, date, queries)
                for date in date_range
            ]
            for future in as_completed(futures):
                data.extend(future.result())

        data = pd.DataFrame(data)

        return data

if __name__ == "__main__":
   
    base_url = "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/records"
    params = {"filter": "example"}

    headers = {}

    query_list = [
        ('refine', "familleavis_lib:\"Procédures collectives\""),
        ('refine', "dateparution:2025-03-02")
    ]

    # Créer une instance du client API
    bodacc_api_client = BodaccAPIClient(base_url, headers=headers)

    # Appeler la méthode pour récupérer toutes les données
    all_data = bodacc_api_client.fetch_all_data_from_api(query_list=query_list, headers=headers)
    # Afficher le nombre total de résultats récupérés
    print(f"Nombre total de résultats récupérés : {len(all_data)}")
    # Afficher les 5 premiers résultats
    print("Exemples de résultats :")
    for result in all_data[:5]:
        print(result)


   