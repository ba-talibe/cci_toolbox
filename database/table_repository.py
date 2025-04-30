from sqlalchemy import Table, MetaData, select
from sqlalchemy.engine import Engine
from ..utils import logger  # Assure-toi que ton logger est bien importé

class TableRepository:
    def __init__(self, engine: Engine, schema: str, table_name: str):
        """
        Initialise le dépôt avec une connexion à la base de données, un schéma et un nom de table.
        :param engine: instance SQLAlchemy Engine
        :param schema: nom du schéma
        :param table_name: nom de la table
        """
        if not isinstance(engine, Engine):
            raise ValueError("L'argument 'engine' doit être une instance de SQLAlchemy Engine.")
        if not isinstance(schema, str) or not isinstance(table_name, str):
            raise ValueError("Les arguments 'schema' et 'table_name' doivent être des chaînes de caractères.")
        if not schema or not table_name:
            raise ValueError("Le schéma et le nom de la table ne peuvent pas être vides.")


        self.engine = engine
        self.schema = schema
        self.table_name = table_name
        self.metadata = MetaData()
        self.table = Table(table_name, self.metadata, autoload_with=engine, schema=schema)
        logger.info(f"Table '{schema}.{table_name}' chargée avec succès.")

    def find_all(self, limit=100):
        """
        Récupère toutes les lignes de la table avec une limite optionnelle.
        :param limit: nombre max de résultats
        :return: liste de lignes
        """

        stmt = select(self.table).limit(limit)
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            return result.fetchall()

    def find_by_column(self, column_name: str, value):
        if column_name not in self.table.columns:
            raise ValueError(f"La colonne '{column_name}' n'existe pas dans la table.")
        
        stmt = select(self.table).where(self.table.c[column_name] == value)
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            return result.fetchall()
    
    def find_by_conditions(self, conditions: dict, logical_operator='AND', limit=100):
        """
        Récupère les lignes correspondant aux conditions.
        :param conditions: dict avec {colonne: valeur}
        :param logical_operator: 'AND' ou 'OR'
        :param limit: nombre max de résultats
        :return: liste de lignes
        """
        if not conditions:
            raise ValueError("Aucune condition fournie.")

        invalid_cols = [col for col in conditions if col not in self.table.columns]
        if invalid_cols:
            raise ValueError(f"Colonnes inexistantes : {invalid_cols}")

        expressions = [
            self.table.c[col] == val
            for col, val in conditions.items()
        ]

        if logical_operator.upper() == 'AND':
            where_clause = and_(*expressions)
        elif logical_operator.upper() == 'OR':
            where_clause = or_(*expressions)
        else:
            raise ValueError("logical_operator doit être 'AND' ou 'OR'")

        stmt = select(self.table).where(where_clause).limit(limit)

        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            return result.fetchall()

    def raw_query(self, stmt):
        """Permet d'exécuter une requête SQLAlchemy manuellement construite sur la table."""
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            return result.fetchall()
