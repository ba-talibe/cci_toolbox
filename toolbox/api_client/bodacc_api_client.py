import os
import json
import requests
import pandas as pd
from .api_client import APIClient
from datetime import datetime
from urllib.parse import urlencode
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..schemas.bodacc_schemas import (
    UnProcessedProcedureCollective,
    ProcessedProcedureCollective,
    UnProcessedVenteCession,
    ProcessedVenteCession
)





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

        # Ajout fr pointeur de fonction pour assurer la compatibilité avec les anciens appels

        self.fetch_and_clean_api_data = self.fetch_and_reduce_ps_data


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
        print(f"Requête initiale : {first_url}")
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
        if self.logger:
            self.logger.info(f"Offsets à traiter : {offsets}")
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

    
    def fetch_data_for_date(self, date,  familleavis_lib=None, queries=None):
        """
        Appelle l'API pour une date donnée et structure les résultats.

        :param date: Date au format 'YYYY-MM-DD'.
        :param headers: En-têtes à utiliser pour la requête.
        :param queries: Liste de tuples de requêtes supplémentaires.
        :return: Liste de dictionnaires contenant les données récupérées.
        """

        cache_key = self._generate_cache_key(date, queries)
        cached_data = self._read_cache(cache_key)
        if cached_data is not None:
            if self.logger:
                self.logger.info(f"Cache utilisé pour la date {date}")
            return cached_data
    
        query_list = [
                    ('refine', 'familleavis_lib:"' + familleavis_lib + '"'),
                    ('refine', f"dateparution:{date}"),
                ]


        if queries:
            query_list.extend(queries)

        try:
            fetched_data = self.fetch_all_data_from_api(
                query_list=query_list
            )
            print(fetched_data)
            data_class = None
            if familleavis_lib == "Procédures collectives":
                data_class = UnProcessedProcedureCollective
            elif familleavis_lib == "Ventes et cessions":
                data_class = UnProcessedVenteCession
            else:
                data_class = None

            if data_class is None:
                
                return fetched_data
            processed_data = [
               data_class.from_dict(row) for row in fetched_data
            ]

            self._write_cache(cache_key, processed_data)
            return processed_data
        except Exception as e:
            print(f"❌ Erreur pour la date {date}: {e}")
            return []

    def fetch_data_since_date(self, start_date, interesting_columns=None, end_date=datetime.now(), queries=None, familleavis_lib=None):
        """
        Récupère les données de l'API depuis une date donnée jusqu'à aujourd'hui.

        :param start_date: Date de début au format 'YYYY-MM-DD'.
        :param queries: Liste de tuples de requêtes supplémentaires.
        :param familleavis_lib: Libellé de la famille d'avis à filtrer.
        :param end_date: Date de fin au format 'YYYY-MM-DD'. Par défaut,
        :return: DataFrame contenant les données récupérées.
        """
        # 2. Créer une plage de dates journalières entre D et aujourd'hui
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        date_range = date_range.strftime("%Y-%m-%d").tolist()  


        
        print()
        data = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(self.fetch_data_for_date, date, familleavis_lib, queries)
                for date in date_range
            ]
            for future in as_completed(futures):
                data.extend(future.result())
        return pd.DataFrame(data)

    
    def fetch_and_reduce_ps_data(self, start_date, queries=None, max_workers=5, end_date=datetime.now()):
        """
        Récupère et nettoie les données de l'API pour un département donné.
        """


        familleavis_lib = "Procédures collectives"
        interesting_columns = UnProcessedProcedureCollective.get_fields()
        # 1. Récupérer les données depuis la date de début jusqu'à aujourd'hui
        return self.fetch_data_since_date(start_date, interesting_columns, end_date, queries, familleavis_lib)

    
    def fetch_and_reduce_vc_data(self, start_date, queries=None, max_workers=5, end_date=datetime.now()):
        """
        Récupère et nettoie les données de l'API pour les ventes et cessions.
        """
        familleavis_lib = "Ventes et cessions"
        interesting_columns = UnProcessedVenteCession.get_fields()
        # 1. Récupérer les données depuis la date de début jusqu'à aujourd'hui
        print(interesting_columns)
        return self.fetch_data_since_date(start_date, interesting_columns, end_date, queries, familleavis_lib)

    
    def fetch_data_for_sirens(self, sirens, queries=None, max_workers=5, familleavis_lib=None):
        """
        Appelle l'API pour une liste de SIRENs et structure les résultats.

        :param sirens: Liste de SIRENs à interroger.
        :param queries: Liste de tuples de requêtes supplémentaires.
        :return: Liste de dictionnaires contenant les données récupérées.
        """
        cache_key = self._generate_cache_key(sirens, queries)
        cached_data = self._read_cache(cache_key)
        if cached_data is not None:
            if self.logger:
                self.logger.info(f"Cache utilisé pour les SIRENs {sirens}")
            return cached_data

        base_queries = [
            ('refine', 'familleavis_lib:"' + familleavis_lib + '"'),
        ]
        if queries:
            base_queries.extend(queries)

        data = pd.DataFrame()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            query_model = base_queries.copy()
            query_list =[query_model + [('refine', f"registre:{siren}")] for siren in sirens]
            futures = [
                executor.submit(self.fetch_all_data_from_api, query)
                for query in query_list
            ]
            for future in as_completed(futures):
                try:
                    data = pd.concat([data, pd.DataFrame(future.result())], ignore_index=True)
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"Erreur lors de la récupération des données pour un SIREN : {e}")
                        self.logger.error(f"Requête échouée : {future}")
        return data




   