import pytest
import pandas as pd
from toolbox.api_client.bodacc_api_client import BodaccAPIClient
from toolbox.data_processing.bodacc_utils import *

# --- Fixtures de test ---

@pytest.fixture
def raw_dataframe():
    client = BodaccAPIClient(
        base_url="https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/records"
    )
    data = client.fetch_and_reduce_ps_data("2025-05-01")
    return pd.DataFrame(data)


@pytest.fixture
def df_with_jugement_extracted():
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


def test_process_judgements_columns(df_with_jugement_extracted):
    df = clean_columns(df_with_jugement_extracted)
    df = clean_dates(df)
    df = process_judgements_columns(df)

    # Test présence de colonnes de statut
    assert "date de plan de redressement" in df.columns
    assert "date ouverture de liquidation judiciaire" in df.columns

    # Vérifie qu'une date a bien été copiée
    assert df["date de plan de redressement"].notnull().sum() > 1
    assert df["date ouverture de liquidation judiciaire"].notnull().sum() > 1


def test_clean_and_extract_ps(raw_dataframe):
    cleaned = clean_and_extract_ps(raw_dataframe.copy())

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
