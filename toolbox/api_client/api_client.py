import os
import abc
import json
import requests
from typing import Dict, Any, Optional



class APIClient(abc.ABC):

    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None, logger: Optional[Any] = None, cache_dir: Optional[str] = None):
        """
        Initialise le client API avec l'URL de base et les en-têtes par défaut. 
        assert base_url, "L'URL de base ne peut pas être vide."

        :param base_url: URL de base de l'API.
        :param headers: En-têtes par défaut à utiliser pour les requêtes.
        """
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        default_headers =  {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        if headers:
            default_headers.update(headers)
        self.session.headers.update(default_headers)
        self.logger = logger
        self.cache_dir = cache_dir
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        
      
    def _get_cache_filepath(self, key: str) -> str:
        os.makedirs(self.cache_dir, exist_ok=True)
        return os.path.join(self.cache_dir, f"{key}.json")

    def _read_cache(self, key: str) -> Optional[list]:
        filepath = self._get_cache_filepath(key)
        if os.path.exists(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Impossible de lire le cache pour {key}: {e}")
        return None

    def _write_cache(self, key: str, data: list):
        filepath = self._get_cache_filepath(key)
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            if self.logger:
                self.logger.error(f"Impossible d'écrire le cache pour {key}: {e}")
    