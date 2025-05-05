import os
import re
from .database import Database
from typing import Optional, Dict, Any, List, Tuple

class BaseRepository:
    """
    Classe de base pour tous les repositories.
    """

    def __init__(self):
        self.conn = Database.get_connection()
        self.limit = 100
        self.offset = 0
        self.limit_pattern = re.compile(r"LIMIT\s+(\d+)", re.IGNORECASE)

    def execute(self, query: str, params: tuple = ()) -> None:
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            self.conn.commit()

    def fetch_one(self, query: str, params: tuple = ()) -> Any:
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()

    def fetch_all(self, query: str, params: tuple = ()) -> list:
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def get_schemas(self) -> List[str]:
            """
            Retourne la liste des schémas disponibles.
            """
            query = "SELECT schema_name FROM information_schema.schemata;"
            result = self.fetch_all(query)
            return [row[0] for row in result]

    def get_tables(self, schema: str = 'public') -> List[str]:
        """
        Retourne la liste des tables du schéma donné.
        """
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
        """
        result = self.fetch_all(query, (schema,))
        return [row[0] for row in result]

    def get_columns(self, table: str, schema: str = 'public') -> List[Tuple[str, str, str]]:
        """
        Retourne les colonnes d'une table : nom, type, nullable.
        """
        query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
        """
        return self.fetch_all(query, (schema, table))



class EtablissementRepository(BaseRepository):
    
    def __init__(self, schema="dm_etab_ent", table="cci_etab_nie"):
        super().__init__()
        self.schema = schema
        self.table = table
        self.table_name = f"{self.schema}.{self.table}"
       

    def get_etablissement_by_siret(self, siret: str, columns=None,) -> Optional[Dict[str, Any]]:
        """
        Récupère un établissement par son SIRET.
        """
        if columns is None:
            columns = "*"
        else:
            columns_string = ",".join(columns)

        query =f"SELECT {columns_string} FROM {self.table_name} WHERE siret=%s;"
        result = self.fetch_one(query, (siret,))
        
        if result:
            return dict(zip(columns, result))
        return None
    
    def get_some_etablissements(self, columns=None, limit=None, offset=None) -> Optional[Dict[str, Any]]:
        """
        Récupère un établissement par son SIRET.
        """
        if columns is None:
            columns = "*"
        else:
            columns_string = ",".join(columns)

        if limit is None:
            limit = self.limit
        if offset is None:
            offset = self.offset

        query = f"SELECT {columns_string} FROM {self.table_name}  LIMIT %s OFFSET %s;"
        results = self.fetch_all(query, (limit, offset))

        if results:
            if columns == "*":
                columns = [desc[0] for desc in self.conn.cursor().description]
            return [dict(zip(columns, result)) for result in results]
            
        return None


if __name__ == "__main__":
    # Exemple d'utilisation
    import os
    from dotenv import load_dotenv

    load_dotenv()

    Database.connect("dbname=ccin_baseco user=usr_ccin_stage  host=10.254.10.3 port=5432")
    repository = BaseRepository()
    etablissement_repository = EtablissementRepository()

    schemas = repository.get_schemas()
    print("Schemas:", schemas)