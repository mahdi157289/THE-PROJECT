from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class BronzeCotation:
    seance: str
    groupe: str
    code: str
    valeur: str
    ouverture: str
    cloture: str
    plus_bas: str
    plus_haut: str
    quantite_negociee: str
    nb_transaction: str
    capitaux: str

@dataclass
class BronzeIndice:
    seance: str
    code_indice: int
    lib_indice: str
    indice_jour: str
    indice_veille: str
    variation_veille: str
    indice_plus_haut: str
    indice_plus_bas: str
    indice_ouv: str

@dataclass
class SilverCotation:
    seance: datetime.date
    groupe: str
    code: str
    valeur: str
    ouverture: float
    cloture: float
    plus_bas: float
    plus_haut: float
    quantite_negociee: float
    nb_transaction: int
    capitaux: float
    annee: int
    mois: int
    jour_semaine: int
    trimestre: int
    est_debut_mois: bool
    est_fin_mois: bool
    price_variation_abs: float  
    variation_pourcentage: float
    ingestion_timestamp: datetime
    source_file: str

@dataclass
class SilverIndice:
    seance: datetime.date
    code_indice: int
    lib_indice: str
    indice_jour: float
    indice_veille: float
    variation_veille: float
    indice_plus_haut: float
    indice_plus_bas: float
    indice_ouv: float
    annee: int
    mois: int
    jour_semaine: int
    trimestre: int
    est_debut_mois: bool
    est_fin_mois: bool
    variation_indice: float
    ingestion_timestamp: datetime
    source_file: str

@dataclass
class RejectedRecord:
    seance: str
    groupe: str
    code: str
    valeur: str
    ouverture: str
    cloture: str
    plus_bas: str
    plus_haut: str
    quantite_negociee: str
    nb_transaction: str
    capitaux: str
    rejection_reason: str
    rejection_timestamp: datetime
    source_file: str
    
 
@dataclass
class RejectedPrimeRecord:
    seance: str
    groupe: str
    code: str
    valeur: str
    ouverture: str
    cloture: str
    plus_bas: str
    plus_haut: str
    quantite_negociee: str
    nb_transaction: str
    capitaux: str
    rejection_reason: str  # e.g., "NULL_VALUE_IN_FIELD_X"
    rejection_timestamp: datetime
    source_file: str   
        