"""
Database module - SQL Server connection and attendance record retrieval.
"""

import logging
import pyodbc
from config import Config

logger = logging.getLogger("hikvision_kelio")


def get_connection(config: Config):
    """
    Create and return a pyodbc connection to SQL Server.

    Args:
        config: Application configuration with DB credentials.

    Returns:
        pyodbc.Connection
    """
    logger.info(f"Connexion à SQL Server: {config.db_host}/{config.db_name}")
    conn = pyodbc.connect(config.db_connection_string)
    logger.info("Connexion à SQL Server réussie")
    return conn


def get_pending_records(config: Config, limit: int = 50, connection=None):
    """
    Fetch a batch of attendance records not yet uploaded (isUploaded = 0).

    Args:
        config: Application configuration.
        limit: Maximum number of records to fetch per batch.
        connection: Optional existing connection. If None, a new one is created.

    Returns:
        list[dict]: List of attendance records (up to `limit`).
    """
    close_conn = False
    if connection is None:
        connection = get_connection(config)
        close_conn = True

    try:
        cursor = connection.cursor()
        query = f"""
            SELECT TOP {limit}
                serialNo,
                emplyeeID,
                authDateTime,
                authDate,
                authTime,
                direction,
                deviceName,
                deviceSerialNo,
                PersonName,
                CardN,
                uploaded
            FROM [{config.db_table}]
            WHERE isUploaded = 0
            ORDER BY authDateTime ASC
        """
        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description]
        records = []
        for row in cursor.fetchall():
            records.append(dict(zip(columns, row)))

        logger.info(f"{len(records)} enregistrement(s) récupéré(s) (lot de {limit})")
        return records

    finally:
        if close_conn:
            connection.close()


def mark_as_uploaded(config: Config, serial_numbers: list, connection=None):
    """
    Mark records as uploaded by setting isUploaded = 1.

    Args:
        config: Application configuration.
        serial_numbers: List of serialNo values to mark.
        connection: Optional existing connection.

    Returns:
        int: Number of rows updated.
    """
    if not serial_numbers:
        return 0

    close_conn = False
    if connection is None:
        connection = get_connection(config)
        close_conn = True

    try:
        cursor = connection.cursor()

        # Use parameterized query with batching for large lists
        batch_size = 500
        total_updated = 0

        for i in range(0, len(serial_numbers), batch_size):
            batch = serial_numbers[i : i + batch_size]
            placeholders = ",".join(["?"] * len(batch))
            query = f"""
                UPDATE [{config.db_table}]
                SET isUploaded = 1
                WHERE serialNo IN ({placeholders})
            """
            cursor.execute(query, batch)
            total_updated += cursor.rowcount

        connection.commit()
        logger.info(f"{total_updated} enregistrement(s) marqué(s) comme uploadé(s) (isUploaded=1)")
        return total_updated

    finally:
        if close_conn:
            connection.close()
