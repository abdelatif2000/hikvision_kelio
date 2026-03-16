"""
Absence Database module — simplified direct connection.

Connects directly to the GEDRHP table in the absence database.
Uses pagination (OFFSET/FETCH) to retrieve records in pages.
"""

import logging
import pyodbc

from config import Config

logger = logging.getLogger("hikvision_kelio")


def get_absence_connection(config: Config):
    """
    Create and return a pyodbc connection to the absence database.

    Args:
        config: Application configuration with absence DB credentials.

    Returns:
        pyodbc.Connection
    """
    conn_str = config.absence_db_connection_string
    logger.info(f"Connexion à la base absence: {config.absence_db_name}")
    return pyodbc.connect(conn_str)


def get_absences_page(config: Config, offset: int, limit: int, connection):
    """
    Fetch a page of absence records from the GEDRHP table.

    Uses OFFSET/FETCH NEXT for pagination. Returns records where
    Statut is 'Validée' or 'Annulée' and Date_Début >= computed start date.

    Args:
        config: Application configuration.
        offset: Number of rows to skip (for pagination).
        limit: Maximum number of rows to return.
        connection: Active DB connection.

    Returns:
        list[dict]: List of absence records for this page.
    """
    cursor = connection.cursor()

    date_from = config.absence_date_from
    table = config.absence_db_table

    # Build optional Code_Entite filter
    entite_filter = ""
    if config.absence_code_entite.strip():
        codes = [c.strip() for c in config.absence_code_entite.split(",") if c.strip()]
        if codes:
            codes_sql = ", ".join(f"N'{c}'" for c in codes)
            entite_filter = f"AND Code_Entite IN ({codes_sql})"

    query = f"""
        SELECT
            ID_Demande,
            Matricule,
            Code_Entite,
            Nom_Complet,
            ID_Type_Demande,
            Date_Début AS Date_Debut,
            IsDateDebutAM,
            Date_fin,
            IsDateFinAM,
            Statut,
            Date_Creation,
            MatriculeCreateur,
            Objet,
            Date_Validation_Finale
        FROM [{table}]
        WHERE Statut IN (N'Validée', N'Annulée')
          AND Date_Début >= '{date_from}'
          {entite_filter}
        ORDER BY Date_Début ASC
        OFFSET {offset} ROWS
        FETCH NEXT {limit} ROWS ONLY
    """
    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]
    rows = []
    for row in cursor.fetchall():
        rows.append(dict(zip(columns, row)))

    logger.info(
        f"Page offset={offset}: {len(rows)} enregistrement(s) récupéré(s)"
    )
    return rows
