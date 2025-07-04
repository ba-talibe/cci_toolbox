from dataclasses import dataclass
from typing import Optional, List, Dict, Any



@dataclass
class UnProcessedProcedureCollective:
    """
    Représente une procédure collective non traitée.
    Cette classe est utilisée pour stocker les données brutes d'une procédure collective avant traitement.
    Les champs sont basés sur les colonnes de la table "annonces commerciales" de l'API BODACC.
    Les champs optionnels sont utilisés pour les données qui peuvent ne pas être présentes dans toutes les annonces.
    Les types de données sont choisis pour correspondre aux types attendus dans les données BODACC.
    """
    id: str
    dateparution: str
    numerodepartement: str
    commercant: str
    jugement: str
    numeroannonce: str
    SIREN: Optional[str] = None

    @classmethod
    def get_fields(cls) -> List[str]:
        """
        Retourne la liste des champs de la classe UnProcessedVenteCession.

        :return: Liste des champs.
        """
        return [field.name for field in cls.__dataclass_fields__.values()]

    @classmethod
    def from_dict(cls, row: Dict[str, Any]) -> 'UnProcessedProcédureCollective':
        """
        Crée une instance de UnProcessedProcédureCollective à partir d'une ligne de résultat SQL.

        :param row: Dictionnaire représentant une ligne de résultat SQL.
        :return: Instance de UnProcessedProcédureCollective.
        """
        return cls(
            id=row.get('id'),
            dateparution=row.get('dateparution'),
            numerodepartement=row.get('numerodepartement'),
            commercant=row.get('commercant'),
            jugement=row.get('jugement'),
            numeroannonce=row.get('numeroannonce'),
            SIREN=row.get("registre")[0] if row.get("registre") else None
        )


@dataclass
class ProcessedProcedureCollective:
    """
    Représente une procédure collective traitée.
    Cette classe est utilisée pour stocker les données d'une procédure collective après traitement.
    Les champs sont basés sur les colonnes de la table "annonces commerciales" de l'API BODACC.
    Les champs optionnels sont utilisés pour les données qui peuvent ne pas être présentes dans toutes les annonces.
    """
    # id,date_parution,numero_departement,raison_sociale,jugement,numero_annonce,siren,date,complement_jugement,type,famille,nature,date_plan_continuation,date_plan_redressement,date_plan_sauvegarde,date_ouverture_une_procedure_sauvegarde,date_modification_plan_redressement,date_ouverture_liquidation_judiciaire,date_mettant_fin_procedure_redressement_judiciaire,date_conversion_en_redressement_judiciaire_procedure,date_extension_procedure_redressement_judiciaire,date_prononcant_resolution_plan_redressement,date_ouverture_procedure_redressement,date_modification_plan_sauvegarde,arret_cour_appel,date_prevue_fin_redressement,date_prevue_fin_sauvegarde

    id: Optional[str] = None
    date_parution: Optional[str] = None
    numero_departement: Optional[str] = None
    raison_sociale: Optional[str] = None
    jugement: Optional[str] = None
    numero_annonce: Optional[str] = None
    siren: Optional[str] = None
    date: Optional[str] = None
    complement_jugement: Optional[str] = None
    type: Optional[str] = None
    famille: Optional[str] = None
    nature: Optional[str] = None
    date_plan_continuation: Optional[str] = None
    date_plan_redressement: Optional[str] = None
    date_plan_sauvegarde: Optional[str] = None
    date_ouverture_une_procedure_sauvegarde: Optional[str] = None
    date_modification_plan_redressement: Optional[str] = None
    date_ouverture_liquidation_judiciaire: Optional[str] = None
    date_mettant_fin_procedure_redressement_judiciaire: Optional[str] = None
    date_conversion_en_redressement_judiciaire_procedure: Optional[str] = None
    date_extension_procedure_redressement_judiciaire: Optional[str] = None
    date_prononcant_resolution_plan_redressement: Optional[str] = None
    date_ouverture_procedure_redressement: Optional[str] = None
    date_modification_plan_sauvegarde: Optional[str] = None
    arret_cour_appel: Optional[str] = None
    date_prevue_fin_redressement: Optional[str] = None
    date_prevue_fin_sauvegarde: Optional[str] = None

@dataclass
class UnProcessedVenteCession:
    """
    Représente une vente ou cession. 
    """
    # 'id', 'dateparution', 'numeroannonce', 'typeavis', 'typeavis_lib', 'familleavis', 'familleavis_lib', 'numerodepartement', 'departement_nom_officiel',
    # 'region_code', 'region_nom_officiel', 'tribunal', 'commercant', 'ville', 'regiAnye', 'cp', 'pdf_parution_subfolder', 'listepersonnes', 'listeetablissements', 
    # 'jugement', 'acte', 'modificationsgenerales', 'radiationaurcs', 'depot', 'listeprecedentexploitant', 'listeprecedentproprietaire', 'divers', 'parutionavisprecedent'
    id: Optional[Any] = None
    dateparution: Optional[Any] = None
    numeroannonce : Optional[Any] = None
    numerodepartement: Optional[Any] = None
    typeavis: Optional[Any] = None
    typeavis_lib: Optional[Any] = None
    familleavis: Optional[Any] = None
    familleavis_lib: Optional[Any] = None
    departement_nom_officiel: Optional[Any] = None
    region_code: Optional[Any] = None
    region_nom_officiel: Optional[Any] = None
    tribunal: Optional[Any] = None
    commercant: Optional[Any] = None
    ville: Optional[Any] = None
    regiAnye: Optional[Any] = None
    cp: Optional[Any] = None
    listepersonnes: Optional[Any] = None
    listeetablissements: Optional[Any] = None
    jugement: Optional[Any] = None    
    acte : Optional[Any] = None
    modificationsgenerales: Optional[Any] = None
    radiationaurcs: Optional[Any] = None
    depot: Optional[Any] = None
    listeprecedentexploitant: Optional[Any] = None
    listeprecedentproprietaire: Optional[Any] = None
    divers: Optional[Any] = None
    parutionavisprecedent: Optional[Any] = None

    @classmethod
    def get_fields(cls) -> List[str]:
        """
        Retourne la liste des champs de la classe UnProcessedVenteCession.

        :return: Liste des champs.
        """
        return [field.name for field in cls.__dataclass_fields__.values()]
    
    @classmethod
    def from_dict(cls, row: Dict[str, Any]) -> 'UnProcessedVenteCession':
        """
        Crée une instance de UnProcessedVenteCession à partir d'une ligne de résultat SQL.

        :param row: Dictionnaire représentant une ligne de résultat SQL.
        :return: Instance de UnProcessedVenteCession.
        """
        return cls(
            id=row.get('id'),
            dateparution=row.get('dateparution'),
            numeroannonce=row.get('numeroannonce'),
            numerodepartement=row.get('numerodepartement'),
            typeavis=row.get('typeavis'),
            typeavis_lib=row.get('typeavis_lib'),
            familleavis=row.get('familleavis'),
            familleavis_lib=row.get('familleavis_lib'),
            departement_nom_officiel=row.get('departement_nom_officiel'),
            region_code=row.get('region_code'),
            region_nom_officiel=row.get('region_nom_officiel'),
            tribunal=row.get('tribunal'),
            commercant=row.get('commercant'),
            ville=row.get('ville'),
            regiAnye=row.get('regiAnye'),
            cp=row.get('cp'),
            listepersonnes=row.get('listepersonnes'),
            listeetablissements=row.get('listeetablissements'),
            jugement=row.get('jugement'),
            acte=row.get('acte'),
            modificationsgenerales=row.get('modificationsgenerales'),
            radiationaurcs=row.get('radiationaurcs'),
            depot=row.get('depot'),
            listeprecedentexploitant=row.get('listeprecedentexploitant'),
            listeprecedentproprietaire=row.get('listeprecedentproprietaire'),
            divers=row.get('divers'),
            parutionavisprecedent=row.get('parutionavisprecedent')
        )
    
@dataclass
class ProcessedVenteCession:
    """
    Représente une vente ou cession traitée.
    Cette classe est utilisée pour stocker les données d'une vente ou cession après traitement.
    Les champs sont basés sur les colonnes de la table "annonces commerciales" de l'API BODACC.
    """
    id: str
    date_parution: str
    numero_departement: str
    raison_sociale: str
    jugement: str
    numero_annonce: str
    siren: str
    date: str
    complement_jugement: Optional[str] = None