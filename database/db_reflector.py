from sqlalchemy import create_engine, MetaData, Table, text, Column
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from geoalchemy2 import Geometry
from typing import Optional, List

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