import pytest
import pandas as pd
from datetime import datetime
from pandas.testing import assert_frame_equal
from toolbox.data_processing.bodacc_utils import *

# --- Fixtures de test ---

@pytest.fixture
def raw_dataframe():
    data = {
        "id": ["A202300013493", "A202300013512", "A202300013491"],
        "registre": ["['841774730', '841 774 730']", "['422090886', '422 090 886']", "['889029500', '889 029 500']"],
        "dateparution": ["2023-01-02"] * 3,
        "numerodepartement": [76, 76, 76],
        "commercant": ["ALR MOTORS", "ARTS DIFFUSION LOISIRS", "LES 2"],
        "jugement": [
            '{"type": "initial", "famille": "Jugement prononçant", "nature": "Jugement de conversion en liquidation judiciaire", "date": "2022-12-27", "complementJugement": "Jugement prononçant la liquidation judiciaire désignant liquidateur Maître Béatrice PASCUAL..."}',
            '{"type": "initial", "famille": "Avis de dépôt", "nature": "Liste des créances nées après le jugement d\'ouverture d\'une procédure de liquidation judiciaire", "date": "2022-12-22", "complementJugement": "La liste des créances de l\'article L 641-13 du code de commerce..."}',
            '{"type": "initial", "famille": "Jugement d\'ouverture", "nature": "Jugement d\'ouverture de liquidation judiciaire", "date": "2022-12-16", "complementJugement": "Jugement prononçant la liquidation judiciaire..."}'
        ],
        "numeroannonce": [3493, 3512, 3491],
        "SIREN": ["841774730", "422090886", "889029500"],
    }

    return pd.DataFrame(data)


@pytest.fixture
def df_with_jugement_extracted(raw_dataframe):
    return extract_jugement_variable(raw_dataframe.copy())


# --- Tests ---

def test_extract_jugement_variable(df_with_jugement_extracted):
    df = df_with_jugement_extracted
    assert "date" in df.columns
    assert "nature" in df.columns
    assert df.loc[0, "nature"] == "Jugement de conversion en liquidation judiciaire"
    assert df.loc[1, "famille"] == "Avis de dépôt"


def test_clean_columns(df_with_jugement_extracted):
    df = clean_columns(df_with_jugement_extracted)
    assert "SIREN" in df.columns
    assert df.loc[0, "SIREN"] == "841774730"
    assert ";" not in df["commercant"].iloc[0]
    assert ";" not in df["complementJugement"].iloc[0]


def test_clean_dates(df_with_jugement_extracted):
    df = clean_dates(df_with_jugement_extracted.copy())
    assert pd.api.types.is_datetime64_any_dtype(df["date"])
    assert df["date"].iloc[0] == pd.Timestamp("2022-12-27")


def test_process_judgements_contains(df_with_jugement_extracted):
    df = clean_columns(df_with_jugement_extracted)
    df = clean_dates(df)
    df = process_judgements_contains(df)

    # Test présence de colonnes de statut
    assert "date de plan de redressement" in df.columns
    assert "date ouverture de liquidation judiciaire" in df.columns

    # Vérifie qu'une date a bien été copiée
    assert df["date de plan de redressement"].notnull().sum() == 1
    assert df["date ouverture de liquidation judiciaire"].notnull().sum() == 1


def test_clean_cleaning_pipeline(raw_dataframe):
    cleaned = clean_cleaning_pipeline(raw_dataframe.copy())

    # Test colonnes clés
    expected_cols = [
        "date de plan de redressement",
        "date ouverture de liquidation judiciaire",
        "SIREN",
        "nature",
        "complementJugement"
    ]
    for col in expected_cols:
        assert col in cleaned.columns

    # Test type de données sur une colonne date
    assert pd.api.types.is_datetime64_any_dtype(cleaned["date de plan de redressement"])
