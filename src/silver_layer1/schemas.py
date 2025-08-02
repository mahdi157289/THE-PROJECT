def get_silver_cotations_schema():
    return """
    CREATE TABLE IF NOT EXISTS silver_cotations (
        seance DATE NOT NULL,
        groupe VARCHAR NOT NULL,
        code VARCHAR NOT NULL,
        valeur VARCHAR NOT NULL,
        ouverture DOUBLE PRECISION NOT NULL,
        cloture DOUBLE PRECISION NOT NULL,
        plus_bas DOUBLE PRECISION NOT NULL,
        plus_haut DOUBLE PRECISION NOT NULL,
        quantite_negociee DOUBLE PRECISION NOT NULL,
        nb_transaction INTEGER NOT NULL,
        capitaux DOUBLE PRECISION NOT NULL,
        annee INTEGER NOT NULL,
        mois INTEGER NOT NULL,
        jour_semaine INTEGER NOT NULL,
        trimestre INTEGER NOT NULL,
        est_debut_mois BOOLEAN NOT NULL,
        est_fin_mois BOOLEAN NOT NULL,
        price_variation_abs DOUBLE PRECISION NOT NULL, 
        variation_pourcentage DOUBLE PRECISION NOT NULL,
        ingestion_timestamp TIMESTAMP NOT NULL,
        source_file VARCHAR NOT NULL
    );
    """

def get_silver_indices_schema():
    return """
    CREATE TABLE IF NOT EXISTS silver_indices (
        seance DATE NOT NULL,
        code_indice BIGINT NOT NULL,
        lib_indice VARCHAR NOT NULL,
        indice_jour DOUBLE PRECISION NOT NULL,
        indice_veille DOUBLE PRECISION NOT NULL,
        variation_veille DOUBLE PRECISION NOT NULL,
        indice_plus_haut DOUBLE PRECISION NOT NULL,
        indice_plus_bas DOUBLE PRECISION NOT NULL,
        indice_ouv DOUBLE PRECISION NOT NULL,
        annee INTEGER NOT NULL,
        mois INTEGER NOT NULL,
        jour_semaine INTEGER NOT NULL,
        trimestre INTEGER NOT NULL,
        est_debut_mois BOOLEAN NOT NULL,
        est_fin_mois BOOLEAN NOT NULL ,
        variation_indice DOUBLE PRECISION NOT NULL,
        ingestion_timestamp TIMESTAMP NOT NULL,
        source_file VARCHAR NOT NULL
    );
    """

def get_rejected_schema():
    return """
    CREATE TABLE IF NOT EXISTS rejected_cotations (
        seance VARCHAR,
        groupe VARCHAR,
        code VARCHAR,
        valeur VARCHAR,
        ouverture VARCHAR,
        cloture VARCHAR,
        plus_bas VARCHAR,
        plus_haut VARCHAR,
        quantite_negociee VARCHAR,
        nb_transaction VARCHAR,
        capitaux VARCHAR,
        annee INTEGER,
        rejection_reason TEXT,
        rejection_timestamp TIMESTAMP,
        source_file VARCHAR
    );
    """
def get_rejected_prime_schema():
    return """
    CREATE TABLE IF NOT EXISTS rejected_prime (
        seance VARCHAR,
        groupe VARCHAR,
        code VARCHAR,
        valeur VARCHAR,
        ouverture VARCHAR,
        cloture VARCHAR,
        plus_bas VARCHAR,
        plus_haut VARCHAR,
        quantite_negociee VARCHAR,
        nb_transaction VARCHAR,
        capitaux VARCHAR,
        rejection_reason TEXT NOT NULL,
        rejection_timestamp TIMESTAMP NOT NULL,
        source_file VARCHAR NOT NULL,
        annee INTEGER,
        mois INTEGER,
        jour_semaine INTEGER,
        trimestre INTEGER,
        est_debut_mois BOOLEAN,
        est_fin_mois BOOLEAN,
        price_variation_abs DOUBLE PRECISION,
        variation_pourcentage DOUBLE PRECISION,
        ingestion_timestamp TIMESTAMP,
        code_indice BIGINT,
        lib_indice VARCHAR,
        indice_jour DOUBLE PRECISION,
        indice_veille DOUBLE PRECISION,
        variation_veille DOUBLE PRECISION,
        indice_plus_haut DOUBLE PRECISION,
        indice_plus_bas DOUBLE PRECISION,
        indice_ouv DOUBLE PRECISION,
        variation_indice DOUBLE PRECISION,
        zero_value_columns TEXT 
    );
    """