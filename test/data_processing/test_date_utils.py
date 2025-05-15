import pytest
from datetime import datetime
import pandas as pd
from toolbox.data_processing.date_utils import (
    mois_fr_vers_num,
    inverse_date,
    remplacer_nombres_francais,
    extraire_annees,
    extraire_mois,
    bi_date
)

def test_mois_fr_vers_num():
    assert mois_fr_vers_num("janvier") == 1
    assert mois_fr_vers_num("Août") == 8
    assert mois_fr_vers_num("décembre") == 12
    assert mois_fr_vers_num("non-mois") is None

def test_inverse_date():
    assert inverse_date("15/04/2024") == "2024-04-15"
    print(inverse_date("2024-04-15"))
    assert inverse_date("2024-04-15") is None
    assert inverse_date("pas une date") is None

def test_remplacer_nombres_francais():
    assert remplacer_nombres_francais("j'ai trois pommes") == "j'ai 3 pommes"
    assert remplacer_nombres_francais("dix-sept oranges") == "17 oranges"
    assert remplacer_nombres_francais("aucun nombre ici") == "aucun nombre ici"
    assert remplacer_nombres_francais("") == ""

def test_extraire_annees():
    assert extraire_annees("durée 5 ans") == 5
    assert extraire_annees("3 an") == 3
    assert extraire_annees("aucune durée") is None

def test_extraire_mois():
    assert extraire_mois("durée 6 mois") == 6
    assert extraire_mois("contrat 12 mois") == 12
    assert extraire_mois("rien à extraire") is None

def test_bi_date():
    assert bi_date("2020-02-29") == datetime(2020, 3, 1)
    assert bi_date("2021-02-28") == datetime(2021, 2, 28)
    assert bi_date(None) is None
