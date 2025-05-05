import os
import re
import psycopg2
from psycopg2.extensions import connection as _connection
from typing import Any, Dict, List, Optional, Tuple
from ..utils import get_logger

def parse_connection_string(connection_string: str) -> dict:
    """
    Convertit une chaîne de connexion PostgreSQL style psql (ex: "dbname=xxx user=xxx") en dictionnaire.

    Exemple :
        "dbname=ccin_baseco user=usr_ccin_stage host=10.254.10.3 port=5432"
        ⟶
        {
            'dbname': 'ccin_baseco',
            'user': 'usr_ccin_stage',
            'host': '10.254.10.3',
            'port': '5432'
        }
    """
    parts = connection_string.strip().split()
    conn_dict = {}
    for part in parts:
        if '=' in part:
            key, value = part.split('=', 1)
            conn_dict[key] = value
    return conn_dict


class Database:
    """
    Singleton pour gérer une connexion unique à PostgreSQL.
    """
    _connection: _connection = None

    @classmethod
    def connect(cls, connection_string: str = "", logger=get_logger) -> _connection:
        if cls._connection is None:
            try:
                cls._connection = psycopg2.connect(connection_string)
                logger.info("Connexion à la base de données réussie.")
            except Exception as e:
                logger.exception("Échec de la connexion à la base de données.")
                raise e
        return cls._connection

    @classmethod
    def get_connection(cls) -> _connection:
        if cls._connection is None:
            logger.error("Aucune connexion à la base de données. Veuillez appeler connect() d'abord.")
            raise Exception("Base de données non connectée. Appelle connect() d'abord.")
        return cls._connection

    @classmethod
    def close(cls):
        if cls._connection:
            cls._connection.close()
            logger.info("Connexion à la base de données fermée.")
            cls._connection = None
