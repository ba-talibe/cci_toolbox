import os
import yaml
import dotenv
# Charger les variables d'environnement depuis un fichier .env
dotenv.load_dotenv()

# Charger la configuration
def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

# Exemple d'utilisation
config = load_config()
# print(config)

if "cci_database" not in config:
  config["cci_database"] = {
    "dbname": os.getenv("BASECO_DB_NAME"),
    "host": os.getenv("BASECO_DB_HOST"),
    "port": os.getenv("BASECO_DB_PORT", "5432"),
    "user": os.getenv("BASECO_DB_USER"),
  }
  if "BASECO_DB_PASSWORD" in os.environ:
    config["cci_database"]["password"] = os.getenv("BASECO_DB_PASSWORD")

if "local_database" not in config:
  config["local_database"] = {
    "dbname": os.getenv("DB_NAME"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "user": os.getenv("DB_USER"),
  }
  if "LOCAL_DB_PASSWORD" in os.environ:
    config["local_database"]["password"] = os.getenv("DB_PASSWORD")