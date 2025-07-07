# Boîte à Outils CCI Toolbox

## Description

La **Boîte à Outils CCI Toolbox** est une collection modulaire de scripts et de fonctions Python destinée à faciliter le traitement de données, l'accès aux API publiques (BODACC, SIREN/SIRET), et l'interaction avec des bases de données PostgreSQL. Elle vise à accélérer le développement de pipelines de données pour les Chambres de Commerce et d'Industrie (CCI) et à garantir la reproductibilité et la robustesse des traitements.

## Fonctionnalités principales

- **Traitement de données** : Nettoyage, normalisation, extraction d'informations à partir de textes, gestion des dates, validation de numéros SIREN/SIRET, etc.
- **Clients API** : Accès simplifié et cache local pour les API BODACC et SIREN/SIRET.
- **Gestion de base de données** : Connexion, introspection, et manipulation de tables PostgreSQL avec SQLAlchemy ou psycopg2.
- **Utilitaires** : Logger configurable, gestion de configuration YAML, outils de manipulation de chaînes de caractères.

## Structure du projet

```
toolbox/
    api_client/
        bodacc_api_client.py      # Client pour l'API BODACC
        siren_api_client.py       # Client pour l'API SIREN/SIRET
        api_client.py             # Classe de base pour les clients API
    data_processing/
        bodacc_utils.py           # Fonctions d'extraction et de nettoyage BODACC
        date_utils.py             # Fonctions utilitaires sur les dates
        str_utils.py              # Fonctions utilitaires sur les chaînes
    database/
        database.py               # Singleton de connexion PostgreSQL (psycopg2)
        base_repository.py        # Requêtes SQL génériques
        db_reflector.py           # Réflexion et création de tables (SQLAlchemy)
        table_repository.py       # Accès orienté table (SQLAlchemy)
    schemas/
        bodacc_schemas.py         # Schémas de données pour BODACC
    utils/
        logger.py                 # Logger configuré (console + fichier)
        config.py                 # Chargement de configuration YAML
```

## Installation

1. **Via pip** (recommandé) :
    ```bash
    pip install git+https://github.com/ba-talibe/cci_toolbox.git
    ```

2. **Dépendances** :  
   Voir `requirements.txt` pour la liste complète (pandas, requests, sqlalchemy, psycopg2, etc.).

## Exemples d'utilisation

### 1. Traitement de données BODACC

```python
from toolbox.data_processing import clean_and_extract_ps
import pandas as pd

df = pd.read_csv("bodacc_annonces.csv")
df_clean = clean_and_extract_ps(df)
print(df_clean.head())
```

### 2. Appel à l'API SIREN

```python
from toolbox.api_client import SirenAPIClient

client = SirenAPIClient(base_url="https://api.insee.fr/entreprises/sirene/V3")
data = client.get_data_by_siren("552100554")
print(data)
```

### 3. Connexion à une base PostgreSQL

```python
from toolbox.database import Database, BaseRepository

Database.connect("dbname=[dn_name] user=[user_name] host=[hostname] port=[port]")
repo = BaseRepository()
schemas = repo.get_schemas()
print(schemas)
```

### 4. Utilisation du logger

```python
from toolbox.utils import get_logger

logger = get_logger()
logger.info("Traitement démarré")
```

## Cas d'usage typiques

- Extraction et nettoyage automatisés des annonces BODACC pour la veille économique.
- Enrichissement de bases internes avec des données SIREN/SIRET à jour.
- Construction de pipelines ETL robustes pour l'intégration de données externes.
- Génération de rapports statistiques sur les entreprises d'un territoire.

## Documentation

La documentation détaillée des modules et des fonctions sera bientôt disponible.

## Licence

Ce projet est sous licence [MIT License](LICENSE).