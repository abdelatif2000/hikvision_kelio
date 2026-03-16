"""
Employee Database module — fetch new/changed records from Collaborateur via Linked Server.

Connects to the local Sync_interface_GED_KELIO database, reads from the
remote Collaborateur table via linked server, and compares HASHBYTES
with the local EmployeeSync tracking table to detect new or modified employees.
"""

import logging
import pyodbc

from config import Config

logger = logging.getLogger("hikvision_kelio")

# Columns included in the change-detection hash
_HASH_COLUMNS = [
    "MatriculePaie",
    "MatriculePointeuse",
    "Entite_Juridique",
    "Prenom",
    "Nom",
    "Prenom_Nom",
    "Date_Embauche",
    "TypeDeContrat",
    "status",
    "Date_Naissance",
    "Date_Départ",
    "Departement",
    "Direction",
    "Service",
    "SocieteInterim",
]


def get_employee_connection(config: Config):
    """
    Create and return a pyodbc connection to the Sync DB (local, with write access).

    Args:
        config: Application configuration with sync DB credentials.

    Returns:
        pyodbc.Connection
    """
    conn_str = config.employee_sync_db_connection_string
    logger.info(f"Connexion à la base sync: {config.employee_sync_db_name}")
    return pyodbc.connect(conn_str)


def _build_hash_expression() -> str:
    """Build the HASHBYTES expression that concatenates all tracked columns."""
    # Use ISNULL + CAST to handle NULLs and different data types
    parts = [
        f"ISNULL(CAST(c.[{col}] AS NVARCHAR(MAX)), N'<<NULL>>')"
        for col in _HASH_COLUMNS
    ]
    concat_expr = " + N'|' + ".join(parts)
    return f"HASHBYTES('SHA2_256', {concat_expr})"


def get_employees_page(config: Config, offset: int, limit: int, connection):
    """
    Fetch a page of new or changed employee records.

    Reads from the remote Collaborateur table (via linked server) and
    LEFT JOINs with the local EmployeeSync table. Only returns rows where
    the employee is new (no sync record) or data has changed (hash mismatch).

    Args:
        config: Application configuration.
        offset: Number of rows to skip (for pagination).
        limit: Maximum number of rows to return.
        connection: Active DB connection to the sync DB.

    Returns:
        list[dict]: List of employee records with an extra 'current_hash' field.
    """
    cursor = connection.cursor()
    source_table = config.employee_source_full_table
    sync_table = config.employee_sync_table
    hash_expr = _build_hash_expression()

    # Build optional Date_Embauche filter
    date_filter = ""
    if config.employee_date_embauche_from.strip():
        date_filter = f"AND c.[Date_Embauche] >= '{config.employee_date_embauche_from}'"

    # Build optional Entite_Juridique LIKE filter
    entite_filter = ""
    params = []
    if config.employee_entite_juridique.strip():
        values = [v.strip() for v in config.employee_entite_juridique.split(",") if v.strip()]
        if values:
            like_clauses = []
            for val in values:
                like_clauses.append("c.[Entite_Juridique] LIKE ?")
                params.append(f"%{val}%")
            entite_filter = "AND (" + " OR ".join(like_clauses) + ")"

    query = f"""
        SELECT
            c.[idusers],
            c.[MatriculePaie],
            c.[MatriculePointeuse],
            c.[Nom],
            c.[Prenom],
            c.[Date_Embauche],
            c.[Date_Naissance],
            c.[TypeDeContrat],
            c.[Service],
            c.[Date_Départ],
            c.[Entite_Juridique],
            c.[Departement],
            c.[Direction],
            c.[SocieteInterim],
            c.[status],
            c.[Prenom_Nom],
            {hash_expr} AS current_hash
        FROM {source_table} c
        LEFT JOIN [{sync_table}] s ON s.[idusers] = c.[idusers]
        WHERE (s.[idusers] IS NULL OR s.[data_hash] != {hash_expr})
        {date_filter}
        {entite_filter}
        ORDER BY c.[idusers] DESC
        OFFSET {offset} ROWS
        FETCH NEXT {limit} ROWS ONLY
    """
    cursor.execute(query, params)

    columns = [desc[0] for desc in cursor.description]
    rows = []
    for row in cursor.fetchall():
        rows.append(dict(zip(columns, row)))

    logger.info(
        f"Page offset={offset}: {len(rows)} employé(s) nouveau(x)/modifié(s)"
    )
    return rows


def mark_employees_synced(config: Config, synced_records: list, connection):
    """
    Upsert the hash into EmployeeSync for successfully imported employees.

    Args:
        config: Application configuration.
        synced_records: List of dicts, each must have 'idusers' and 'current_hash'.
        connection: Active DB connection to the sync DB.
    """
    if not synced_records:
        return

    sync_table = config.employee_sync_table
    cursor = connection.cursor()

    for record in synced_records:
        idusers = record.get("idusers")
        current_hash = record.get("current_hash")
        if idusers is None or current_hash is None:
            continue

        cursor.execute(f"""
            MERGE [{sync_table}] AS target
            USING (SELECT ? AS idusers, ? AS data_hash) AS source
            ON target.[idusers] = source.[idusers]
            WHEN MATCHED THEN
                UPDATE SET data_hash = source.data_hash, last_synced = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT ([idusers], [data_hash], [last_synced])
                VALUES (source.idusers, source.data_hash, GETDATE());
        """, (idusers, current_hash))

    connection.commit()
    logger.info(f"{len(synced_records)} employé(s) marqué(s) comme synchronisé(s)")
