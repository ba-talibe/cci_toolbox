
# logger.py
import os
import logging
from logging.handlers import RotatingFileHandler


LOG_DIR = "logs"
LOG_FILE = "app.log"

def get_logger() -> logging.Logger:
    # Créer le dossier s’il n’existe pas
    os.makedirs(LOG_DIR, exist_ok=True)

    # Configuration du logger
    logger = logging.getLogger("app")
    logger.setLevel(logging.DEBUG)  # DEBUG en dev, INFO ou WARNING en prod

    # Format des messages
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    # Handler fichier avec rotation
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, LOG_FILE), maxBytes=1_000_000, backupCount=5
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # handler console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)  # INFO en dev, WARNING ou ERROR en prod
    console_format = logging.Formatter('[%(asctime)s] [%(levelname)s] %(name)s: %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)



    # Ajouter les handlers (évite les doublons si relancé)
    if not logger.handlers:
        logger.addHandler(file_handler)

    return logger
