import re

corrections_caracteres = {
    "Ã§": "ç",
    "ã§": "ç",
    "Ã©": "é",
    "Ã¨": "è",
    "Ãª": "ê",
    "Ã«": "ë",
    "Ã ": "à",
    "Ã¢": "â",
    "Ã¤": "ä",
    "Ã¹": "ù",
    "Ã»": "û",
    "Ã¼": "ü",
    "Ã´": "ô",
    "Ã¶": "ö",
    "Ã®": "î",
    "Ã¯": "ï",
    "ÃŸ": "ß",
    "Ã˜": "Ø",
    "Ã˜": "ø",
    "Ã…": "Å",
    "Ã¥": "å",
    "Ã†": "Æ",
    "Ã¦": "æ",
    "Ã‡": "Ç",
    "Ã‰": "É",
    "Ãˆ": "È",
    "ÃŠ": "Ê",
    "Ã‹": "Ë",
    "Ã€": "À",
    "Ã‚": "Â",
    "Ã„": "Ä",
    "ÃÔ": "Ô",
    "Ã–": "Ö",
    "Ãœ": "Ü",
    "Ã‚": "Â",
    "â€™": "’",
    "â€œ": "“",
    "â€�": "”",
    "â€“": "–",
    "â€”": "—",
    "â€": "†",
    "â€¢": "•",
    "â‚¬": "€",
    "â„¢": "™",
    "â€˜": "‘",
    "â€¡": "‡",
    "Â«": "«",
    "Â»": "»",
    "Â°": "°",
    "Â²": "²",
    "Â³": "³",
    "Âµ": "µ",
    "Â·": "·",
    "Â": "",  # Souvent résidu vide
}

def clean_chaine(chaine):
    """
    Nettoie une chaine de caractère en :
        - remplaçant les '.' par '_'
        - insérant un '_' avant chaque majuscule sauf si elle est en début de chaîne
        - conservant les suites de majuscules comme un seul mot
        - remplaçant les doubles underscores par un seul underscore
        - Convertissant en minuscules
    """
    # Remplacer les '.' par '_'
    chaine = chaine.replace('.', '_')
    # Insérer un '_' avant une majuscule qui suit une minuscule ou un chiffre
    chaine = re.sub(r'(?<=[a-z0-9])([A-Z])', r'_\1', chaine)
    # Remplacer les doubles underscores par un seul underscore
    chaine = re.sub(r'__+', '_', chaine)
    # Convertir en minuscules
    return chaine.lower()



def ajouter_espace(x):
    """
    Ajoute un espace entre les lettres et les chiffres si collés : "TEST123" -> "TEST 123"
    """
    return re.sub(r"(?<=[A-Za-z])(?=\d)|(?<=\d)(?=[A-Za-z])", " ", str(x))




def nettoyage_texte(txt):
    """
    Nettoie et normalise un texte : supprime les doublons d'espaces, met en minuscule
    """
    if not txt:
        return ""
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip().lower()

