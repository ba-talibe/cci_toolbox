

from data_processing import *
from database import DbReflector, parse_connection_string
from utils import config
import pandas as pd

def test_bodacc_utils():
    df = pd.read_csv("p:\\Baseco\\Pipelines\\Robert\\Bodacc\\Travaux CCIN\\data_brute_76.csv")
    
    df = extract_jugement_variable(df)
    df = clean_columns(df)
    df = remove_no_siren_rows(df)

  
    # ouv_ps = ouv_ps.iloc[1:3]
    ouv_ps = filter_and_group(df, "nature", r"ouverture.*procédure de sauvegarde")
    i = 4
    ouv_ps = ouv_ps.loc[ouv_ps.id == "A202300013508", :]
    # Extraction des mois
    cm = []
    textes = []
    # print(ouv_ps["complementJugement"])
    for texte in ouv_ps["complementJugement"]:
        
        extrait = re.search(r"(.{2,10})\smois", str(texte))
        if extrait:
            print(texte)
        brut = extrait.group(1) if extrait else ""
        print("Brut : ", brut)
        print(remplacer_nombres_francais(brut))
        cm.append(remplacer_nombres_francais(brut))
    print(cm)
    mois = []
    for txt in cm:
        extrait = re.search(r"(\d+)", str(txt))
        if extrait:
            print(txt)
        mois.append(int(extrait.group(1)) if extrait else 0)

    print(mois)
    # # Extraction des années
    # cm = []
    # for texte in ouv_ps["complementJugement"]:
    #     extrait = re.search(r"(.{2,10})\s(?:ans|annees|annee|années|année|an|nomme)", str(texte))
    #     brut = extrait.group(1) if extrait else ""
    #     cm.append(remplacer_nombres_francais(brut))

    # annes = []
    # for txt in cm:
    #     extrait = re.search(r"(\d+)", str(txt))
    #     annes.append(int(extrait.group(1)) if extrait else 0)

    # print(annes)

def test_remplacer_nombres_francais():
    tests = {
        "un mois": "1 mois",
        "deux mois": "2 mois",
        "treize mois": "13 mois",
        "vingt mois": "20 mois",
        "dix-huit mois": "18 mois",
        "durée de quinze mois": "durée de 15 mois",
        "aucune durée": "aucune durée",
        "": "",
        None: ""
    }

    for input_text, expected_output in tests.items():
        result = remplacer_nombres_francais(input_text)
        assert result == expected_output, f"Échec pour '{input_text}': obtenu '{result}', attendu '{expected_output}'"

    print("✅ Tous les tests passent !")


def test_db_reflector():

    database_info = parse_connection_string(config["database"]["connection_string"])
    with DBRefletor(host=database_info["host"],
                   user=database_info["user"],
                   dbname=database_info["dbname"]) as reflector:
        
        etablissement_table = reflector.get_table(schema="dm_etab_ent", table_name="cci_etab_nie")

        print(etablissement_table.columns.keys())


if __name__ == "__main__":
   test_db_reflector()