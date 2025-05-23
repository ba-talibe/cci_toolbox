import re

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

    Args:
        variable_name (str): Le nom de la variable à extraire.
    Returns:
        function: Une fonction qui prend une ligne du DataFrame et renvoie la valeur de la variable spécifiée.
    """
    def extract_variable(row):
        try:
            jugement = json.loads(row["jugement"])
            return jugement.get(variable_name, None)
        except (json.JSONDecodeError, KeyError):
            return None
    return extract_variable

def corriger_caracteres_speciaux(text, corrections=corrections_caracteres):
    """
    Corrige les caractères spéciaux dans le texte en utilisant un dictionnaire de corrections.

    Args:
        text (str): Le texte à corriger.
        corrections (dict): Un dictionnaire de corrections où la clé est le mauvais caractère et la valeur est le bon caractère.
    Returns:
        str: Le texte corrigé.
    """
    if pd.isnull(text):
        return text
    for mauvais, bon in corrections.items():
        text = text.replace(mauvais, bon)
    return text

def extract_jugement_variable(dataframe : pd.DataFrame):
    """
    Extrait une variable spécifique du champ 'jugement' dans le DataFrame.

    Args:
        dataframe (pd.DataFrame): Le DataFrame contenant la colonne 'jugement'.
    Returns:
        pd.DataFrame: Le DataFrame avec les champs 'date', 'complementJugement', 'type', 'famille' et 'nature' extraits du champ 'jugement'.
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

    Args:
        df (pd.DataFrame): Le DataFrame à traiter.
    Returns:
        pd.DataFrame: Le DataFrame avec les colonnes int64 converties en str64.
    """
    for col in df.select_dtypes(include=["int64"]).columns:
        df[col] = df[col].astype(str)
    return df


def clean_columns(df):
    """
    Nettoie les colonnes du DataFrame en supprimant les espaces et en remplaçant les caractères spéciaux.

    Args:
        df (pd.DataFrame): Le DataFrame à traiter.
    Returns:
        pd.DataFrame: Le DataFrame avec les colonnes nettoyées.
    """
    assert "registre" in df.columns, "La colonne 'registre' n'existe pas dans le DataFrame."
    assert "commercant" in df.columns, "La colonne 'commercant' n'existe pas dans le DataFrame."
    assert "complementJugement" in df.columns, "La colonne 'complementJugement' n'existe pas dans le DataFrame."
    assert "nature" in df.columns, "La colonne 'nature' n'existe pas dans le DataFrame."
    assert "SIREN" in df.columns, "La colonne 'SIREN' n'existe pas dans le DataFrame."

    def extraire_siren(val):
        if  isinstance(val, str):
            match = re.search(r"'([^']+)'", val)
            if match:
                return match.group(1)
        elif isinstance(val, list):
            if len(val) > 0:
                return val[0]
        return None
    
    df["SIREN"] = df["registre"].apply(extraire_siren)

    # --- Nettoyage ponctuation ---
    df["commercant"] = df["commercant"].str.replace(";", " ", regex=False)
    df["complementJugement"] = df["complementJugement"].str.replace(";", " ", regex=False)
    df["registre"] = df["registre"].str.replace(";", ",", regex=False)
    df["nature"] = df["nature"].str.replace(";", ",", regex=False)

    return df


def filter_and_group(df, col, pattern):
    """
    Filtre le DataFrame en fonction d'un motif dans une colonne donnée et groupe les résultats.

    Args:
        df (pd.DataFrame): Le DataFrame à traiter.
        col (str): Le nom de la colonne à filtrer.
        pattern (str): Le motif à rechercher dans la colonne.
    """
    assert col in df.columns, f"La colonne '{col}' n'existe pas dans le DataFrame."
    assert "SIREN" in df.columns, "La colonne 'SIREN' n'existe pas dans le DataFrame."
    assert "date" in df.columns, "La colonne 'date' n'existe pas dans le DataFrame."


    mask = df[col].str.contains(pattern, case=False, regex=True, na=False)
    return df[mask].sort_values(by=["SIREN", "date"], ascending=[True, False]).drop_duplicates("SIREN")


def rename_columns(df):

    """
    Renomme les colonnes du DataFrame pour les rendre plus lisibles.

    Args:
        df (pd.DataFrame): Le DataFrame à traiter.
    Returns:
        pd.DataFrame: Le DataFrame avec les colonnes renommées.
    """
    df = df.rename(columns={
        "dateparution" : "date_parution",
        "numerodepartement" : "numero_departement",
        "numeroannonce" : "numero_annonce",
        "commercant" : 'raison_sociale',
    })
    return df

def process_judgements_columns(df):
    """
    Traite les colonnes de jugement pour extraire les informations pertinentes.

    Args:
        df (pd.DataFrame): Le DataFrame à traiter.
    Returns:
        pd.DataFrame: Le DataFrame avec les colonnes de jugement traitées.
    """
    assert "SIREN" in df.columns, "La colonne 'SIREN' n'existe pas dans le DataFrame."
    assert "date" in df.columns, "La colonne 'date' n'existe pas dans le DataFrame."
    assert "nature" in df.columns, "La colonne 'nature' n'existe pas dans le DataFrame."
    assert "complementJugement" in df.columns, "La colonne 'complementJugement' n'existe pas dans le DataFrame."


    def ajouter_duree(row):
        """
        Ajoute la durée de redressement ou de sauvegarde à la date de jugement.

        Args:
            row (pd.Series): Une ligne du DataFrame contenant les colonnes 'date', 'duree_annes' et 'duree_mois'.
        Returns:
            datetime: La date de fin de la procédure.
        """
        assert "date" in row, "La colonne 'date' n'existe pas dans la ligne."
        assert "duree_annes" in row, "La colonne 'duree_annes' n'existe pas dans la ligne."
        assert "duree_mois" in row, "La colonne 'duree_mois' n'existe pas dans la ligne."
        base_date = bi_date(row["date"])
        try:
            return base_date + relativedelta(
                years=int(row["duree_annes"] or 0),
                months=int(row["duree_mois"] or 0)
            )
        except Exception:
            return pd.NaT 

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

        
        pr["date_fin"] = pr.apply(ajouter_duree, axis=1)
       

        
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
        ps["date_fin"] = ps.apply(ajouter_duree, axis=1)

        # fusion des données de sauvegarde et de redressement

        
    status = df.copy()
    # 2. Fusion des dates selon chaque procédure
    status["date_plan_continuation"] = status["SIREN"].map(pc.set_index("SIREN")["date"])
    status["date_plan_redressement"] = status["SIREN"].map(pr.set_index("SIREN")["date"])
    status["date_prevue_fin_redressement"] = status["SIREN"].map(pr.set_index("SIREN")["date_fin"])
    status["date_plan_sauvegarde"] = status["SIREN"].map(ps.set_index("SIREN")["date"])
    status["date_prevue_fin_sauvegarde"] = status["SIREN"].map(ps.set_index("SIREN")["date_fin"])
    status["date_ouverture_une_procedure_sauvegarde"] = status["SIREN"].map(ouv_ps.set_index("SIREN")["date"])
    status["date_modification_plan_redressement"] = status["SIREN"].map(mod_p.set_index("SIREN")["date"])
    status["date_ouverture_liquidation_judiciaire"] = status["SIREN"].map(lj.set_index("SIREN")["date"])
    status["date_mettant_fin_procedure_redressement_judiciaire"] = status["SIREN"].map(fpr.set_index("SIREN")["date"])
    status["date_conversion_en_redressement_judiciaire_procedure"] = status["SIREN"].map(xr_to_sauv.set_index("SIREN")["date"])
    status["date_extension_procedure_redressement_judiciaire"] = status["SIREN"].map(xpr.set_index("SIREN")["date"])
    status["date_prononcant_resolution_plan_redressement"] = status["SIREN"].map(pro_pr.set_index("SIREN")["date"])
    status["date_ouverture_procedure_redressement"] = status["SIREN"].map(ouv_pr.set_index("SIREN")["date"])
    status["date_modification_plan_sauvegarde"] = status["SIREN"].map(mod_ps.set_index("SIREN")["date"])
    status["arret_cour_appel"] = status["SIREN"].map(ljc.set_index("SIREN")["date"])
    status = rename_columns(status)
    return status


def remove_no_siren_rows(df):
    """
    Supprime les lignes sans SIREN

    Args:
        df (pd.DataFrame): Le DataFrame à traiter.
    Returns:
        pd.DataFrame: Le DataFrame sans les lignes sans SIREN.
    """
    assert "SIREN" in df.columns, "La colonne 'SIREN' n'existe pas dans le DataFrame."
    return df[~df["SIREN"].isnull() & (df["SIREN"] != "")]

def rename_columns(df):


    """
    Renomme les colonnes du DataFrame pour les rendre plus lisibles.

    Args:
        df (pd.DataFrame): Le DataFrame à traiter.
    Returns:
        pd.DataFrame: Le DataFrame avec les colonnes renommées.
    """
    df = df.rename(columns={
        "dateparution" : "date_parution",
        "commercant" : 'raison_sociale',
        "numerodepartement" : "numero_departement",
        "numeroannonce" : "numero_annonce"
    })
    return df


def clean_and_extract_ps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline de nettoyage des données et extraction des informations sur les procédures de judiciaire.

    Args:
        df (pd.DataFrame): Le DataFrame à traiter.
    Returns:
        pd.DataFrame: Le DataFrame nettoyé et enrichi avec les informations sur les procédures judiciaires.
    """
    if "siren" in df.columns:
        df = df.rename(columns={"siren": "SIREN"})
    df = extract_jugement_variable(df)
    df = clean_columns(df)
    df = remove_no_siren_rows(df)
    df = clean_dates(df)
    df = convert_int_to_str_columns(df)
    df = process_judgements_columns(df)
    df.columns = df.columns.map(clean_chaine)

    return df