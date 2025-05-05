import yaml

# Charger la configuration
def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

# Exemple d'utilisation
config = load_config()
# print(config)