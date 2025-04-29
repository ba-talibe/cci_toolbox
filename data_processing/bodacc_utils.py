import re
from datetime import datetime
import pandas as pd
import json
from dateutil.relativedelta import relativedelta






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

def ajouter_espace(x):
    """
    Ajoute un espace entre les lettres et les chiffres si collés : "TEST123" -> "TEST 123"
    """
    return re.sub(r"(?<=[A-Za-z])(?=\d)|(?<=\d)(?=[A-Za-z])", " ", str(x))


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
        # ✅ Utilise \b sans double échappement
        chaine = re.sub(rf"\b{mot}\b", chiffre, chaine, flags=re.IGNORECASE)

    return chaine

def nettoyage_texte(txt):
    """
    Nettoie et normalise un texte : supprime les doublons d'espaces, met en minuscule
    """
    if not txt:
        return ""
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip().lower()


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


def create_jugement_variable_extractor(variable_name):
    """
    Crée une fonction pour extraire une variable spécifique du champ 'jugement'.
    """
    def extract_variable(row):
        try:
            jugement = json.loads(row["jugement"])
            return jugement.get(variable_name, None)
        except (json.JSONDecodeError, KeyError):
            return None
    return extract_variable

def corriger_caracteres_speciaux(text, corrections=corrections_caracteres):
    if pd.isnull(text):
        return text
    for mauvais, bon in corrections.items():
        text = text.replace(mauvais, bon)
    return text

def extract_jugement_variable(dataframe : pd.DataFrame):
    """
    Extrait une variable spécifique du champ 'jugement' dans le DataFrame.
    """
    assert "jugement" in dataframe.columns, "La colonne 'jugement' n'existe pas dans le DataFrame."

    
# Supposons que dataframe est un DataFrame existant avec une colonne "jugement"
    dataframe["jugement"] = (
        dataframe["jugement"]
        .astype(str)
        .str.replace(r'"{4}', '"', regex=True)       # remplace """" par "
        .str.replace(r'""', '"', regex=True)         # remplace "" par "
        .str.replace(r'""""', "'", regex=True)       # remplace """" par '
        .str.replace(r"\}'", "", regex=True)         # supprime }'
        .str.replace(r'\\"\\\"', '"', regex=True)    # remplace \"\" par "
    )

    # extraction des variable imbriquées dans le champ jugement
    dataframe["date"] = dataframe.apply(create_jugement_variable_extractor("date"), axis=1)
    dataframe["complementJugement"] = dataframe.apply(create_jugement_variable_extractor("complementJugement"), axis=1)
    dataframe["type"] = dataframe.apply(create_jugement_variable_extractor("type"), axis=1)
    dataframe["famille"] = dataframe.apply(create_jugement_variable_extractor("famille"), axis=1)
    dataframe["nature"] = dataframe.apply(create_jugement_variable_extractor("nature"), axis=1)

    
    # nettoyage des variables extraites
    dataframe.loc[:, "nature"] = dataframe.loc[:, "nature"].apply(corriger_caracteres_speciaux)
    dataframe.loc[:, "famille"] = dataframe.loc[:, "famille"].apply(corriger_caracteres_speciaux)

    return dataframe





def clean_columns(df):
    # Extraire les 9 premiers caractères sans espaces
    df["SIREN"] = df["registre"].str.replace(" ", "", regex=False).str[:9]

    # --- Nettoyage ponctuation ---
    df["commercant"] = df["commercant"].str.replace(";", " ", regex=False)
    df["complementJugement"] = df["complementJugement"].str.replace(";", " ", regex=False)
    df["registre"] = df["registre"].str.replace(";", ",", regex=False)
    df["nature"] = df["nature"].str.replace(";", ",", regex=False)

    return df


def filter_and_group(df, col, pattern):
    mask = df[col].str.contains(pattern, case=False, regex=True, na=False)
    return df[mask].sort_values(by=["SIREN", "date"], ascending=[True, False]).drop_duplicates("SIREN")


def process_judgements_contains(df):
    lj = filter_and_group(df, "nature", r"jugement.*liquidation judiciaire")
    ljc = filter_and_group(df, "nature", r"arr..t.*cour.*appel")
    pr = filter_and_group(df, "nature", r"plan de redressement")
    fpr = filter_and_group(df, "nature", r"fin.*redressement judiciaire")
    xpr = filter_and_group(df, "nature", r"extension.*redressement judiciaire")
    xr_to_sauv = filter_and_group(df, "nature", r"conversion.*sauvegarde")
    pro_pr = filter_and_group(df, "nature", r"résolution.*plan de redressement")
    ouv_pr = filter_and_group(df, "nature", r"ouverture.*redressement judiciaire")
    pc = filter_and_group(df, "nature", r"modifiant.*plan de continuation")
    mod_p = filter_and_group(df, "nature", r"modifiant.*plan de redressement")
    ouv_ps = filter_and_group(df, "nature", r"ouverture.*procédure de sauvegarde")
    ps = filter_and_group(df, "nature", r"plan de sauvegarde")
    mod_ps = filter_and_group(df, "nature", r"modifiant.*plan de sauvegarde")



    # --- Extraction durée de redressement ---
    if not pr.empty:
        pr["complementJugement"] = pr["complementJugement"].apply(ajouter_espace)

        # Extraction des années
        cm = []
        for texte in pr["complementJugement"]:
            extrait = re.search(r"(.{2,10})\s(?:ans|annees|annee|années|année|an|nomme)", str(texte))
            brut = extrait.group(1) if extrait else ""
            cm.append(remplacer_nombres_francais(brut))

        annes = []
        for txt in cm:
            extrait = re.search(r"(\d+)", str(txt))
            annes.append(int(extrait.group(1)) if extrait else 0)

        pr["duree_annes"] = annes

        # Extraction des mois
        cm = []
        for texte in pr["complementJugement"]:
            extrait = re.search(r"(.{2,10})\smois", str(texte))
            brut = extrait.group(1) if extrait else ""
            cm.append(remplacer_nombres_francais(brut))

        mois = []
        for txt in cm:
            extrait = re.search(r"(\d+)", str(txt))
            mois.append(int(extrait.group(1)) if extrait else 0)

        pr["duree_mois"] = mois

        # Calcul de la date de fin
        pr["date_fin"] = pr["date"].apply(bi_date) + pr.apply(lambda row: relativedelta(years=row["duree_annes"], months=row["duree_mois"]), axis=1)

        
    # --- Extraction durée de sauvegarde ---
    if not ps.empty:
        ps["complementJugement"] = ps["complementJugement"].apply(ajouter_espace)
        ps = ps[~ps["complementJugement"].str.contains("fin du plan|accélérée", case=False, na=False)]

        cm = []
        for texte in ps["complementJugement"]:
            extrait = re.search(r"(.{3,10})\s(?:an|nommant)", str(texte))
            brut = extrait.group(1) if extrait else ""
            cm.append(remplacer_nombres_francais(brut))

        annes = []
        for txt in cm:
            extrait = re.search(r"(\d+)", str(txt))
            annes.append(int(extrait.group(1)) if extrait else 0)

        ps["duree_annes"] = annes

        # Mois
        cm = []
        for texte in ps["complementJugement"]:
            extrait = re.search(r"(.{4,10})\smois", str(texte))
            brut = extrait.group(1) if extrait else ""
            cm.append(remplacer_nombres_francais(brut))

        mois = []
        for txt in cm:
            extrait = re.search(r"(\d+)", str(txt))
            mois.append(int(extrait.group(1)) if extrait else 0)

        ps["duree_mois"] = mois

        # Calcul de la date de fin
        ps["date_fin"] = ps["date"].apply(bi_date) + ps.apply(lambda row: relativedelta(years=row["duree_annes"], months=row["duree_mois"]), axis=1)


        # fusion des données de sauvegarde et de redressement

        
    status = df.copy()
    # 2. Fusion des dates selon chaque procédure
    status["date plan de continuation"] = status["SIREN"].map(pc.set_index("SIREN")["date"])
    status["date de plan de redressement"] = status["SIREN"].map(pr.set_index("SIREN")["date"])
    status["date prevue fin de redressement"] = status["SIREN"].map(pr.set_index("SIREN")["date_fin"])
    status["date de plan de sauvegarde"] = status["SIREN"].map(ps.set_index("SIREN")["date"])
    status["date prevue fin de sauvegarde"] = status["SIREN"].map(ps.set_index("SIREN")["date_fin"])
    status["date d_ouverture d_une procédure de sauvegarde"] = status["SIREN"].map(ouv_ps.set_index("SIREN")["date"])
    status["date de modification de plan de redressement"] = status["SIREN"].map(mod_p.set_index("SIREN")["date"])
    status["date ouverture de liquidation judiciaire"] = status["SIREN"].map(lj.set_index("SIREN")["date"])
    status["date mettant fin à la procédure de redressement judiciaire"] = status["SIREN"].map(fpr.set_index("SIREN")["date"])
    status["date conversion en redressement judiciaire de la procédure de sauvegarde"] = status["SIREN"].map(xr_to_sauv.set_index("SIREN")["date"])
    status["date extension d_une procédure de redressement judiciaire"] = status["SIREN"].map(xpr.set_index("SIREN")["date"])
    status["date prononçant la résolution du plan de redressement et la liquidation judiciaire"] = status["SIREN"].map(pro_pr.set_index("SIREN")["date"])
    status["date d_ouverture d_une procédure de redressement"] = status["SIREN"].map(ouv_pr.set_index("SIREN")["date"])
    status["date de modification le plan de sauvegarde"] = status["SIREN"].map(mod_ps.set_index("SIREN")["date"])
    status["arrêt de la cour d_appel infirmant une décision soumise à publicité"] = status["SIREN"].map(ljc.set_index("SIREN")["date"])

    return status


def remove_no_siren_rows(df):
    """
    Supprime les lignes sans SIREN
    """
    return df[~df["SIREN"].isnull() & (df["SIREN"] != "")]