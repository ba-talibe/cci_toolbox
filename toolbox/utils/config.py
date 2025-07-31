import os
import yaml
import configparser
import dotenv
# Charger les variables d'environnement depuis un fichier .env
dotenv.load_dotenv()

# Charger la configuration
def load_yaml_config(path="config.yaml"):
    try:
      with open(path, "r") as f:
          return yaml.safe_load(f)
    except Exception as e:
      print(f"Erreur lors du chargement de la configuration depuis {path}: {e}")
      return {}

def load_ini_config(path="config.ini"):
    try:
        config = configparser.ConfigParser()
        config.read(path)
        return {section: dict(config.items(section)) for section in config.sections()}
    except Exception as e:
        print(f"Erreur lors du chargement de la configuration depuis {path}: {e}")
        return {}


# Exemple d'utilisation
config = load_yaml_config()
config.update(load_ini_config())
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