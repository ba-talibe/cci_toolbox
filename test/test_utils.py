import pytest
from toolbox.data_processing import *

@pytest.mark.parametrize("input_text, expected_output", [
    ("un mois", "1 mois"),
    ("deux mois", "2 mois"),
    ("treize mois", "13 mois"),
    ("vingt mois", "20 mois"),
    ("dix-huit mois", "18 mois"),
    ("durée de quinze mois", "durée de 15 mois"),
    ("aucune durée", "aucune durée"),
    ("", ""),
    (None, "")
])
def test_remplacer_nombres_francais(input_text, expected_output):
    assert remplacer_nombres_francais(input_text) == expected_output
