import pandas as pd
from sqlalchemy import Table, MetaData, select, and_, or_, func, null
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from ..utils import get_logger  # Assure-toi que ton logger est bien importé


logger = get_logger()

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
            raise ValueError("Les arguments 'schema' et 'table_name' doivent être des chaînes de caractères.", type(schema), type(table_name))
        if not schema or not table_name:
            raise ValueError("Le schéma et le nom de la table ne peuvent pas être vides.")


        self.engine = engine
        self.schema = schema
        self.table_name = table_name
        self.metadata = MetaData()
        self.table = Table(table_name, self.metadata, autoload_with=engine, schema=schema)
        logger.info(f"Table '{schema}.{table_name}' chargée avec succès.")



    def find_all(self, limit=100, return_df=False):
        """
        Récupère toutes les lignes de la table avec une limite optionnelle.
        :param limit: nombre max de résultats
        :return: liste de lignes
        """

        stmt = select(self.table).limit(limit)
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            return self.__return_df(result) if return_df else result.fetchall()

    def find_by_column(self, column_name: str = None, value=None, columns=None, return_df=False, distinct=False):
        

        # Vérification des colonnes demandées
        if columns:
            if invalid_cols := [
                col for col in columns if col not in self.table.columns
            ]:
                raise ValueError(f"Colonnes demandées non valides : {invalid_cols}")
            selected_columns = [self.table.c[col] for col in columns]
        else:
            selected_columns = [self.table]  # Sélectionne toutes les colonnes
        if distinct:
            selected_columns = [func.distinct(col) for col in selected_columns]
        if column_name:
            if column_name not in self.table.columns:
                raise ValueError(f"La colonne '{column_name}' n'existe pas dans la table.")

            if value is None:
                raise ValueError("La valeur ne peut pas être None si column_name est spécifié.")
            stmt = select(*selected_columns).where(self.table.c[column_name] == value)
        elif value is None:
            stmt = select(*selected_columns)
        else:
            raise ValueError("La valeur doit être None si column_name n'est pas spécifié.")
        
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            return self.__return_df(result) if return_df else result.fetchall()
    
    def find_by_conditions(self, conditions: dict, logical_operator='AND', columns=None, limit=100):
        """
        Récupère les lignes correspondant aux conditions.
        :param conditions: dict avec {colonne: valeur}
        :param logical_operator: 'AND' ou 'OR'
        :param limit: nombre max de résultats
        :return: liste de lignes
        """
        if not conditions:
            raise ValueError("Aucune condition fournie.")

        if invalid_cols := [
            col for col in conditions if col not in self.table.columns
        ]:
            raise ValueError(f"Colonnes inexistantes : {invalid_cols}")

        # Vérification des colonnes demandées
        if columns:
            if invalid_cols := [
                col for col in columns if col not in self.table.columns
            ]:
                raise ValueError(f"Colonnes demandées non valides : {invalid_cols}")
            selected_columns = [self.table.c[col] for col in columns]
        else:
            selected_columns = [self.table]  # Sélectionne toutes les colonnes

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

        stmt = select(*selected_columns).where(where_clause).limit(limit)

        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            return result.fetchall()

    def count(self):
        """Compte le nombre de lignes dans la table."""
        stmt = select([func.count()]).select_from(self.table)
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            return result.scalar()

    def get_columns(self):
        """Récupère les noms des colonnes de la table."""
        return [col.name for col in self.table.columns]

    def raw_query(self, stmt):
        """Permet d'exécuter une requête SQLAlchemy manuellement construite sur la table."""
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            return result.fetchall()
    
    def __is_compatible(self, df: pd.DataFrame):
        """
        Vérifie si le DataFrame est compatible avec la table.
        :param df: pandas DataFrame à vérifier
        :return: bool
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("L'argument 'df' doit être un pandas DataFrame.")
        if not set(df.columns).issuperset(set(self.get_columns())):
            raise ValueError("Les colonnes du DataFrame ne correspondent pas à celles de la table.")
        return True

    def __return_df(self, result):
        """Convertit le résultat en DataFrame."""
        if not result:
            return pd.DataFrame()
        return pd.DataFrame(result.fetchall(), columns=result.keys())

    def upsert(self, df: pd.DataFrame, confilct_col):
        """
        Insère ou met à jour les données d'un DataFrame dans la table.
        :param df: pandas DataFrame à insérer
        :param if_exists: 'fail' | 'replace' | 'append'
        """
        assert self.__is_compatible(df), "Le DataFrame n'est pas compatible avec la table."
        assert isinstance(confilct_col, str), "Le nom de la colonne de conflit doit être une chaîne de caractères."
        if confilct_col not in self.get_columns():
            raise ValueError(f"La colonne '{confilct_col}' n'existe pas dans la table.")
        if not isinstance(df, pd.DataFrame):
            raise ValueError("L'argument 'df' doit être un pandas DataFrame.")
        
        df = df.copy()
        table_columns = self.get_columns()
        for col in df.columns:
            if col not in table_columns:
                df = df.drop(columns=col)
        df = df.fillna(null())

        stmt = insert(self.table).values(df.to_dict(orient='records'))
        update_cols = {col.name: stmt.excluded[col.name] for col in self.table.columns if col.name != confilct_col}

        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=[confilct_col],  # Clé unique
            set_=update_cols
        )

        with self.engine.begin() as conn:
            conn.execute(upsert_stmt)

      