import re
import pandas as pd
from datetime import datetime
import pytz


def mois_fr_vers_num(mois):
    """
    Convertit un mois en français vers un numéro (ex: "janvier" -> 1)
    """
    mois = mois.lower()
    mois_dict = {
        'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4, 'mai': 5, 'juin': 6,
        'juillet': 7, 'août': 8, 'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12
    }
    return mois_dict.get(mois)

def inverse_date(date_str):
    """
    Inverse une date de format JJ/MM/AAAA -> AAAA-MM-JJ
    """
    try:
        d = datetime.strptime(date_str, "%d/%m/%Y")
        return d.strftime("%Y-%m-%d")
    except Exception:
        return None

        

def remplacer_nombres_francais(chaine):
    """
    Remplace les nombres en français par leur équivalent numérique dans une chaîne de caractères.
    """
    correspondances = {
        "onze": "11", "douze": "12", "treize": "13",
        "quatorze": "14", "quinze": "15", "seize": "16",
        "dix-sept": "17", "dix-huit": "18", "dix-neuf": "19", "vingt": "20",
        "un": "1", "deux": "2", "trois": "3", "quatre": "4",
        "cinq": "5", "six": "6", "sept": "7", "huit": "8", "neuf": "9",
        "dix": "10"
    }

    if not chaine:
        return ""

    for mot, chiffre in correspondances.items():
        #  Utilise \b sans double échappement
        chaine = re.sub(rf"\b{mot}\b", chiffre, chaine, flags=re.IGNORECASE)

    return chaine

    
def extraire_annees(txt):
    """
    Extrait une durée en années depuis un texte (ex: "durée 3 ans")
    """
    match = re.search(r"(\d+)\s*(an|ans)", txt, re.IGNORECASE)
    return int(match.group(1)) if match else None



def extraire_mois(txt):
    """
    Extrait une durée en mois depuis un texte (ex: "durée 6 mois")
    """
    match = re.search(r"(\d+)\s*(mois)", txt, re.IGNORECASE)
    return int(match.group(1)) if match else None


def bi_date(d):
    """
    Si la date est un 29 février, retourne le 1er mars de la même année.
    Sinon, retourne la date telle quelle.
    """
    if pd.isnull(d):
        return None
    if isinstance(d, str):
        d = pd.to_datetime(d, errors="coerce")

    if d.month == 2 and d.day == 29:
        return datetime(d.year, 3, 1)
    return d

def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les colonnes de type date dans le DataFrame.
    """
    date_columns = [col for col in df.columns if "date" in col.lower()]

    for col in date_columns:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        except Exception as e:
            print(f"Erreur lors du traitement de la colonne {col}: {e}")

    return df