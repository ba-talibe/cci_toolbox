import pandas as pd
from geoalchemy2 import Geometry
from typing import Optional, List
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, MetaData, Table, text, Column,inspect
from sqlalchemy.types import String, Integer, Float, Boolean, Date




from ..utils import get_logger  # import ton logger depuis logger.py


logger = get_logger()


class DBReflector:
    def __init__(self, user: str, host: str="localhost", port: int = 5432, dbname: str="postgres"):
        
        self.connection_string = f"postgresql+psycopg2://{user}@{host}:{port}/{dbname}"
        self.engine: Optional[Engine] = None
        self.metadata: Optional[MetaData] = None
        self.connection: Optional[Connection] = None

    def _connect(self):
        try:
            self.engine = create_engine(self.connection_string)
            self.connection = self.engine.connect()
            self.metadata = MetaData()
            logger.debug("Connexion réussie à la base de données.")
        except SQLAlchemyError as e:
            logger.error(f"Erreur de connexion : {e}")
            raise

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()
            logger.debug("Connexion fermée proprement.")
        if self.engine:
            self.engine.dispose()
            logger.debug("Engine SQLAlchemy libéré.")
    
    def table_exists(self, schema: str, table_name: str):
        """
        Vérifie si une table existe dans un schéma donné.
        """
        inspector = inspect(self.engine)
        # Vérifie si le nom de la table existe dans les tables du schéma
        return table_name in inspector.get_table_names(schema=schema)

    def get_table(self, table_name: str, schema: Optional[str] = None) -> Optional[Table]:
        try:
            # Charger la table avec réflexion
            return Table(table_name, self.metadata, autoload_with=self.engine, schema=schema)
        except SQLAlchemyError as e:
            logger.error(f"Erreur lors de la réflexion de la table {schema}.{table_name}: {e}")
            return None


    def get_all_schemas(self) -> List[str]:
        if self.engine is None:
            raise RuntimeError("Connexion non initialisée.")

        try:
            with self.engine.connect() as conn:
                result = conn.execute("SELECT schema_name FROM information_schema.schemata")
                schemas = [row[0] for row in result]
                logger.debug(f"Schemas récupérés : {schemas}")
                return schemas
        except SQLAlchemyError as e:
            logger.error(f"Erreur lors de la récupération des schémas : {e}")
            return []

    def get_all_tables(self, schema: Optional[str] = None) -> List[str]:
        if self.engine is None:
            raise RuntimeError("Connexion non initialisée.")

        try:
            with self.engine.connect() as conn:
                query = "SELECT table_name FROM information_schema.tables WHERE table_schema = :schema"
                result = conn.execute(query, {"schema": schema or "public"})
                tables = [row[0] for row in result]
                logger.debug(f"Tables récupérées dans le schéma {schema}: {tables}")
                return tables
        except SQLAlchemyError as e:
            logger.error(f"Erreur lors de la récupération des tables : {e}")
            return []

    def map_dtype(self, dtype):
        """Mappe les dtypes pandas vers des types SQLAlchemy."""
        if pd.api.types.is_integer_dtype(dtype):
            return String()
        elif pd.api.types.is_float_dtype(dtype):
            return String()
        elif pd.api.types.is_bool_dtype(dtype):
            return Boolean()
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return Date()
        else:
            return String()

    def create_table_from_dataframe(self, schema: str, table_name: str, df: pd.DataFrame, if_exists="fail"):
        """
        Crée une table dans la base à partir d'un DataFrame.

        :param schema: nom du schéma cible
        :param table_name: nom de la table à créer
        :param df: pandas DataFrame à utiliser comme modèle
        :param if_exists: 'fail' | 'replace' | 'append'
        """
        metadata = MetaData(schema=schema)
        columns = [
            Column(col_name, self.map_dtype(dtype))
            for col_name, dtype in df.dtypes.items()
        ]

        table = Table(table_name, metadata, *columns)

        if if_exists == "replace":
            table.drop(self.engine, checkfirst=True)

        if if_exists in ["replace", "fail"]:
            table.create(self.engine, checkfirst=(if_exists == "fail"))

        elif if_exists == "append":
            if not self.engine.dialect.has_table(self.engine.connect(), table_name, schema=schema):
                table.create(self.engine)

        return table