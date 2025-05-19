import re
import pytz
import json
import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from .date_utils import *
from .str_utils import *



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

def convert_int_to_str_columns(df):
    """
    Convertit les colonnes de type int64 en str64
    """
    for col in df.select_dtypes(include=["int64"]).columns:
        df[col] = df[col].astype(str)
    return df


def clean_columns(df):
    def extraire_premier_registre(val):
        if not isinstance(val, str):
            return None
        match = re.search(r"'([^']+)'", val)
        if match:
            return match.group(1)
        return None

        df["SIREN"] = df["registre"].apply(extraire_premier_registre)

    # --- Nettoyage ponctuation ---
    df["commercant"] = df["commercant"].str.replace(";", " ", regex=False)
    df["complementJugement"] = df["complementJugement"].str.replace(";", " ", regex=False)
    df["registre"] = df["registre"].str.replace(";", ",", regex=False)
    df["nature"] = df["nature"].str.replace(";", ",", regex=False)

    return df


def filter_and_group(df, col, pattern):
    mask = df[col].str.contains(pattern, case=False, regex=True, na=False)
    return df[mask].sort_values(by=["SIREN", "date"], ascending=[True, False]).drop_duplicates("SIREN")


def process_judgements_columns(df):
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

        try:
            # Calcul de la date de fin
            pr["date_fin"] = pr["date"].apply(bi_date) + pr.apply(lambda row: relativedelta(years=row["duree_annes"], months=row["duree_mois"]), axis=1)
        except Exception as e:
            
            pr["date_fin"] = pd.NaT

        
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
        try:
            ps["date_fin"] =  ps["date"].apply(bi_date) + ps.apply(lambda row: relativedelta(years=row["duree_annes"], months=row["duree_mois"]), axis=1)
        except Exception as e:
            ps["date_fin"] = pd.NaT


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


def clean_dates(df: pd.DataFrame, date_type : str ="date") -> pd.DataFrame:
    paris_tz = pytz.timezone("Europe/Paris")

    date_columns = [col for col in df.columns if "date" in col.lower()]

    for col in date_columns:
        try:
            # Conversion en datetime, erreurs ignorées pour les valeurs non valides
            df[col] = pd.to_datetime(df[col], errors='coerce')

            # Localisation + conversion en ISO 8601 (optionnel selon données)
            if date_type == "date":
                df [col] = pd.to_datetime(df[col], errors='coerce').dt.normalize()
            else:
                df[col] = df[col].dt.tz_localize('Europe/Paris', ambiguous='NaT', nonexistent='NaT', errors='coerce') \
                                .dt.tz_convert('UTC') \
                                .dt.strftime('%Y-%m-%dT%H:%M:%SZ')  # ISO format en UTC
        except Exception as e:
            print(f"Erreur lors du traitement de la colonne {col}: {e}")

    return df

def clean_cleaning_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline de nettoyage des données
    """
    df = extract_jugement_variable(df)
    df = clean_columns(df)
    df = remove_no_siren_rows(df)
    df = clean_dates(df)
    df = convert_int_to_str_columns(df)
    df = process_judgements_columns(df)

    return df