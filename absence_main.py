"""
Absence File Import — GEDRHP → Kelio
======================================
Main orchestrator: fetches absence records from SQL Server (GEDRHP table),
filters by status, and imports/deletes them in Kelio using pagination.

Usage:
    python absence_main.py
"""

import sys
import os
import logging
import time

# Ensure the script can find local modules when run directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_absence_config
from absence_db import get_absence_connection, get_absences_page
from absence_kelio_service import AbsenceKelioService
from email_alert import send_error_alert


def setup_logging(level: str = "INFO"):
    """Configure logging with console and file handlers."""
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(
        log_dir,
        f"absence_kelio_{time.strftime('%Y%m%d_%H%M%S')}.log",
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
        config = get_absence_config()
    except ValueError as e:
        print(f"Erreur de configuration:\n{e}")
        sys.exit(1)

    logger = setup_logging(config.log_level)
    logger.info("=" * 60)
    logger.info("  Absence Import — GEDRHP → Kelio")
    logger.info(f"  Date de début: {config.absence_date_from}")
    logger.info(f"  Mois en arrière: {config.absence_months_back}")
    logger.info(f"  Code_Entite: {config.absence_code_entite or 'TOUS'}")
    logger.info(f"  Taille de lot: {config.batch_size}")
    logger.info("=" * 60)

    # ── 2. Connect to SQL Server ───────────────────────────────────
    try:
        db_conn = get_absence_connection(config)
    except Exception as e:
        logger.exception(f"Impossible de se connecter à SQL Server: {e}")
        sys.exit(1)

    # ── 3. Connect to Kelio AbsenceFileService ─────────────────────
    kelio = AbsenceKelioService(config)
    if not kelio.connect():
        logger.error("Impossible de se connecter au service Absence Kelio. Abandon.")
        db_conn.close()
        sys.exit(1)

    total_imported = 0
    total_deleted = 0
    total_fail = 0
    all_error_details = []

    # ── 4. Process records page by page ────────────────────────────
    offset = 0
    page_num = 0

    while True:
        page_num += 1
        logger.info(f"--- Page {page_num} (offset={offset}) ---")

        try:
            records = get_absences_page(
                config, offset=offset, limit=config.batch_size, connection=db_conn
            )
        except Exception as e:
            logger.exception(f"Erreur récupération page {page_num}: {e}")
            break

        if not records:
            logger.info("Plus d'enregistrements à traiter.")
            break

        # Split by status
        validees = [r for r in records if r.get("Statut") == "Validée"]
        annulees = [r for r in records if r.get("Statut") == "Annulée"]

        logger.info(
            f"Page {page_num}: {len(records)} enregistrement(s) "
            f"({len(validees)} Validée, {len(annulees)} Annulée)"
        )

        # ── Import Validée records in bulk ─────────────────────────
        if validees:
            success_ids, failed_ids, errors = kelio.import_absence_files(validees)
            total_imported += len(success_ids)
            total_fail += len(failed_ids)
            if errors:
                all_error_details.extend(errors)

        # ── Delete Annulée records in bulk ─────────────────────────
        if annulees:
            success_ids, failed_ids, errors = kelio.delete_absence_files(annulees)
            total_deleted += len(success_ids)
            total_fail += len(failed_ids)
            if errors:
                all_error_details.extend(errors)

        # Move to next page
        offset += config.batch_size

    # ── 5. Summary ─────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(f"  Résultat:")
    logger.info(f"    Importées (Validée) : {total_imported}")
    logger.info(f"    Supprimées (Annulée): {total_deleted}")
    logger.info(f"    Rejetées            : {total_fail}")
    logger.info(
        f"    Total traité: {total_imported + total_deleted + total_fail}"
    )
    if all_error_details:
        logger.warning("  Absences rejetées par Kelio:")
        for err in all_error_details:
            emp_id = err.get("employeeIdentificationNumber", "?")
            err_msg = err.get("errorMessage", "Unknown error")
            logger.warning(f"    Matricule {emp_id} : {err_msg}")
    logger.info("=" * 60)

    # ── 6. Send email alert if errors ──────────────────────────────
    if all_error_details:
        send_error_alert(
            subject="Kelio — Erreurs Absences",
            error_details=all_error_details,
            config=config,
        )

    db_conn.close()
    logger.info("Terminé.")


if __name__ == "__main__":
    main()
