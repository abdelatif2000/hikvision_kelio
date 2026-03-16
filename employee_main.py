"""
Employee Import — Collaborateur → Kelio
=========================================
Main orchestrator: fetches employee records from SQL Server (Collaborateur table)
and imports them into Kelio using EmployeeService + EmployeeFieldService.

Usage:
    python employee_main.py
"""

import sys
import os
import logging
import time

# Ensure the script can find local modules when run directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_employee_config
from employee_db import get_employee_connection, get_employees_page, mark_employees_synced
from employee_kelio_service import EmployeeKelioService
from email_alert import send_error_alert


def setup_logging(level: str = "INFO"):
    """Configure logging with console and file handlers."""
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(
        log_dir,
        f"employee_kelio_{time.strftime('%Y%m%d_%H%M%S')}.log",
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
        config = get_employee_config()
    except ValueError as e:
        print(f"Erreur de configuration:\n{e}")
        sys.exit(1)

    logger = setup_logging(config.log_level)
    logger.info("=" * 60)
    logger.info("  Employee Import — Collaborateur → Kelio (delta sync)")
    logger.info(f"  Sync DB: {config.employee_sync_db_name}")
    logger.info(f"  Source: {config.employee_source_full_table}")
    logger.info(f"  Section par défaut: {config.employee_default_section}")
    date_filter = config.employee_date_embauche_from or "TOUS (pas de filtre)"
    logger.info(f"  Filtre Date_Embauche >=: {date_filter}")
    entite_filter = config.employee_entite_juridique or "TOUS (pas de filtre)"
    logger.info(f"  Filtre Entite_Juridique: {entite_filter}")
    logger.info(f"  Taille de lot: {config.batch_size}")
    logger.info("=" * 60)

    # ── 2. Connect to SQL Server ───────────────────────────────────
    try:
        db_conn = get_employee_connection(config)
    except Exception as e:
        logger.exception(f"Impossible de se connecter à SQL Server: {e}")
        sys.exit(1)

    # ── 3. Connect to Kelio EmployeeService + EmployeeFieldService ─
    kelio = EmployeeKelioService(config)
    if not kelio.connect():
        logger.error("Impossible de se connecter aux services Employee Kelio. Abandon.")
        db_conn.close()
        sys.exit(1)

    total_emp_imported = 0
    total_emp_failed = 0
    total_field_imported = 0
    total_field_failed = 0
    total_synced = 0
    all_error_details = []

    # ── 4. Process records page by page ────────────────────────────
    offset = 0
    page_num = 0

    while True:
        page_num += 1
        logger.info(f"--- Page {page_num} (offset={offset}) ---")

        try:
            records = get_employees_page(
                config, offset=offset, limit=config.batch_size, connection=db_conn
            )
        except Exception as e:
            logger.exception(f"Erreur récupération page {page_num}: {e}")
            break

        if not records:
            logger.info("Plus d'enregistrements à traiter.")
            break

        logger.info(f"Page {page_num}: {len(records)} employé(s) nouveau(x)/modifié(s) à traiter")

        # ── Import employees in bulk ───────────────────────────────
        success, failed, errors = kelio.import_employees(records)
        total_emp_imported += len(success)
        total_emp_failed += len(failed)
        if errors:
            all_error_details.extend(errors)

        # ── Import employee free fields (Bp) in bulk ───────────────
        f_success, f_failed, f_errors = kelio.import_employee_fields(records)
        total_field_imported += len(f_success)
        total_field_failed += len(f_failed)
        if f_errors:
            all_error_details.extend(f_errors)

        # ── Mark successfully imported employees as synced ─────────
        # Only mark employees that succeeded in BOTH employee + field import
        success_set = set(success)
        f_success_set = set(f_success)
        fully_synced_matricules = success_set & f_success_set

        synced_records = [
            r for r in records
            if str(r.get("MatriculePointeuse", "")).strip() in fully_synced_matricules
            and r.get("current_hash") is not None
        ]
        if synced_records:
            try:
                mark_employees_synced(config, synced_records, db_conn)
                total_synced += len(synced_records)
            except Exception as e:
                logger.error(f"Erreur mise à jour sync pour page {page_num}: {e}")

        # Move to next page
        offset += config.batch_size

    # ── 5. Summary ─────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(f"  Résultat:")
    logger.info(f"    Employés importés       : {total_emp_imported}")
    logger.info(f"    Employés rejetés        : {total_emp_failed}")
    logger.info(f"    Champs Bp importés      : {total_field_imported}")
    logger.info(f"    Champs Bp rejetés       : {total_field_failed}")
    logger.info(f"    Employés synchronisés   : {total_synced}")
    logger.info(
        f"    Total traité: {total_emp_imported + total_emp_failed}"
    )
    if all_error_details:
        logger.warning("  Détails des erreurs Kelio:")
        for err in all_error_details:
            mat = err.get("MatriculePointeuse", "?")
            err_msg = err.get("errorMessage", "Unknown error")
            logger.warning(f"    MatriculePointeuse={mat} : {err_msg}")
    logger.info("=" * 60)

    # ── 6. Send email alert if errors ──────────────────────────────
    if all_error_details:
        send_error_alert(
            subject="Kelio — Erreurs Employés",
            error_details=all_error_details,
            config=config,
        )

    db_conn.close()
    logger.info("Terminé.")


if __name__ == "__main__":
    main()
