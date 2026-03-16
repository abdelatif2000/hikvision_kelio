"""
HikVision SQL Server -> Kelio Integration
==========================================
Main orchestrator: fetches attendance records from SQL Server
and sends them to the Kelio Web Service.

Usage:
    python main.py
"""

import sys
import os
import logging
import time

# Ensure the script can find local modules when run directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_config
from db import get_connection, get_pending_records, mark_as_uploaded
from kelio_service import KelioService
from email_alert import send_error_alert


def setup_logging(level: str = "INFO"):
    """Configure logging with console and file handlers."""
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(
        log_dir,
        f"hikvision_kelio_{time.strftime('%Y%m%d_%H%M%S')}.log",
    )

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    return logging.getLogger("hikvision_kelio")


def main():
    """Main entry point."""
    # ── 1. Load configuration ──────────────────────────────────────
    try:
        config = get_config()
    except ValueError as e:
        print(f"Erreur de configuration:\n{e}")
        sys.exit(1)

    logger = setup_logging(config.log_level)
    logger.info("=" * 60)
    logger.info("  HikVision -> Kelio  —  Démarrage de l'importation")
    logger.info("=" * 60)

    # ── 2. Connect to SQL Server ───────────────────────────────────
    try:
        db_conn = get_connection(config)
    except Exception as e:
        logger.exception(f"Impossible de se connecter à SQL Server: {e}")
        sys.exit(1)

    # ── 3. Connect to Kelio SOAP ───────────────────────────────────
    kelio = KelioService(config)
    if not kelio.connect():
        logger.error("Impossible de se connecter au service Kelio. Abandon.")
        db_conn.close()
        sys.exit(1)

    # ── 4. Process records batch by batch ──────────────────────────
    total_success = 0
    total_fail = 0
    batch_num = 0
    all_error_details = []  # Collect {employeeIdentificationNumber, errorMessage} dicts

    while True:
        # Fetch one batch from DB
        try:
            records = get_pending_records(config, limit=config.batch_size, connection=db_conn)
        except Exception as e:
            logger.exception(f"Erreur lors de la récupération des pointages: {e}")
            break

        if not records:
            logger.info("Plus de pointages en attente.")
            break

        batch_num += 1
        logger.info(
            f"Lot {batch_num}: {len(records)} enregistrement(s) récupéré(s), envoi vers Kelio…"
        )

        # Send batch to Kelio
        success_serials, failed_serials, error_details = kelio.send_clockings_batch(records)

        # Mark successful records as uploaded
        if success_serials:
            try:
                mark_as_uploaded(config, success_serials, connection=db_conn)
            except Exception as e:
                logger.error(
                    f"Erreur lors de la mise à jour du statut uploaded: {e}"
                )

        # Track error details for final summary
        if error_details:
            all_error_details.extend(error_details)
            # Also mark failed records so they don't block the next batch
            try:
                mark_as_uploaded(config, failed_serials, connection=db_conn)
                logger.info(
                    f"{len(failed_serials)} pointage(s) rejeté(s) marqué(s) comme "
                    f"uploadé(s) (employé inexistant dans Kelio)"
                )
            except Exception as e:
                logger.error(
                    f"Erreur lors du marquage des pointages rejetés: {e}"
                )

        total_success += len(success_serials)
        total_fail += len(failed_serials)

    # ── 5. Summary ─────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(f"  Résultat: {total_success} succès / {total_fail} rejeté(s)")
    logger.info(f"  Total traité: {total_success + total_fail} ({batch_num} lots)")
    if all_error_details:
        logger.warning("  Pointages rejetés par Kelio:")
        for err in all_error_details:
            emp_id = err.get("employeeIdentificationNumber", "?")
            err_msg = err.get("errorMessage", "Unknown error")
            logger.warning(f"    Identification number {emp_id} : {err_msg}")
    logger.info("=" * 60)

    # ── 6. Send email alert if errors ──────────────────────────────
    if all_error_details:
        send_error_alert(
            subject="Kelio — Erreurs Pointages",
            error_details=all_error_details,
            config=config,
        )

    db_conn.close()
    logger.info("Terminé.")


if __name__ == "__main__":
    main()
