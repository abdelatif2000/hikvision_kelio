"""
Kelio AbsenceFileService SOAP module — imports and deletes absence files in Kelio.

Uses the same zeep + HTTPBasicAuth pattern as kelio_service.py.
Embeds #ID_Demande in the comment field for precise error matching.
"""

import logging
import os
from datetime import datetime
from lxml import etree

from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client, Settings
from zeep.transports import Transport
from zeep.plugins import HistoryPlugin

from config import Config

logger = logging.getLogger("hikvision_kelio")


class AbsenceKelioService:
    """Client for the Bodet Kelio AbsenceFileService SOAP API."""

    def __init__(self, config: Config):
        self.config = config
        self.client = None
        self.history = HistoryPlugin()
        self._log_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "logs"
        )

    # ── Connection ───────────────────────────────────────────────────

    def connect(self) -> bool:
        """
        Establish the SOAP connection to Kelio AbsenceFileService.

        Returns:
            bool: True if connection succeeded.
        """
        try:
            session = Session()
            if self.config.kelio_soap_login and self.config.kelio_soap_password:
                session.auth = HTTPBasicAuth(
                    self.config.kelio_soap_login,
                    self.config.kelio_soap_password,
                )

            settings = Settings(strict=False)

            self.client = Client(
                self.config.kelio_absence_soap_url,
                transport=Transport(session=session),
                settings=settings,
                plugins=[self.history],
            )
            logger.info(
                f"Client SOAP Absence connecté à {self.config.kelio_absence_soap_url}"
            )
            return True

        except Exception as e:
            logger.exception(
                f"Erreur de connexion au service SOAP Absence Kelio: {e}"
            )
            return False

    # ── Import Validée records (bulk with comment-based matching) ────

    def import_absence_files(self, records: list) -> tuple:
        """
        Import validated absence records into Kelio in bulk.

        Embeds #ID_Demande in the comment field so we can precisely match
        errors in the SOAP response back to specific records.

        Args:
            records: List of record dicts from GEDRHP with Statut='Validée'.

        Returns:
            (success_ids, failed_ids, error_details):
                success_ids: list of ID_Demande values imported OK.
                failed_ids: list of ID_Demande values rejected.
                error_details: list of error dicts.
        """
        if not self.client:
            logger.error("Client SOAP non connecté — appelez connect() d'abord")
            return [], [r.get("ID_Demande") for r in records], []

        # Build AbsenceFile objects
        absence_files = []
        id_map = []  # Track ID_Demande for each absence file
        for record in records:
            try:
                absence_file = self._build_absence_file(record)
                absence_files.append(absence_file)
                id_map.append(record.get("ID_Demande"))
            except Exception as e:
                logger.error(
                    f"Erreur préparation absence ID={record.get('ID_Demande')}: {e}"
                )

        if not absence_files:
            return [], [r.get("ID_Demande") for r in records], []

        # Call importAbsenceFiles
        try:
            request = {
                "absenceFilesToImport": {
                    "AbsenceFile": absence_files,
                }
            }
            result = self.client.service.importAbsenceFiles(**request)

            # Parse errors and match by #ID_Demande in comment
            errors = self._extract_errors(result)

            if not errors:
                logger.info(
                    f"Lot de {len(absence_files)} absence(s) importé(s) avec succès"
                )
                return id_map, [], []

            # Match errors back to ID_Demande via the comment field
            failed_id_set = set()
            error_details = []
            for err in errors:
                comment = err.get("comment", "")
                err_msg = err.get("errorMessage", "Unknown error")
                emp_id = err.get("employeeIdentificationNumber", "?")

                # Extract #ID_Demande from comment
                matched_id = self._extract_id_from_comment(comment)
                if matched_id:
                    failed_id_set.add(matched_id)
                    logger.warning(
                        f"Absence REJETÉE: ID={matched_id}, Matricule={emp_id} — {err_msg}"
                    )
                else:
                    # Fallback: can't match, log the error
                    logger.warning(
                        f"Absence REJETÉE: Matricule={emp_id} — {err_msg} (comment: {comment})"
                    )

                error_details.append({
                    "ID_Demande": matched_id,
                    "employeeIdentificationNumber": emp_id,
                    "errorMessage": err_msg,
                })

            # Split into success/fail
            success_ids = [id_d for id_d in id_map if id_d not in failed_id_set]
            failed_ids = [id_d for id_d in id_map if id_d in failed_id_set]

            logger.info(
                f"Import: {len(success_ids)} succès, {len(failed_ids)} rejeté(s)"
            )
            return success_ids, failed_ids, error_details

        except Exception as e:
            logger.error(f"Erreur envoi lot de {len(absence_files)} absences: {e}")
            return [], id_map, []

    # ── Delete Annulée records (bulk with comment-based matching) ────

    def delete_absence_files(self, records: list) -> tuple:
        """
        Delete (cancel) absence records in Kelio for Annulée entries.

        Args:
            records: List of record dicts from GEDRHP with Statut='Annulée'.

        Returns:
            (success_ids, failed_ids, error_details)
        """
        if not self.client:
            logger.error("Client SOAP non connecté — appelez connect() d'abord")
            return [], [r.get("ID_Demande") for r in records], []

        absence_files = []
        id_map = []
        for record in records:
            try:
                absence_file = self._build_absence_file(record)
                absence_files.append(absence_file)
                id_map.append(record.get("ID_Demande"))
            except Exception as e:
                logger.error(
                    f"Erreur préparation suppression absence ID={record.get('ID_Demande')}: {e}"
                )

        if not absence_files:
            return [], [r.get("ID_Demande") for r in records], []

        # Call deleteAbsenceFilesBetweenTwoDates
        try:
            request = {
                "absenceFilesWithDates": {
                    "AbsenceFile": absence_files,
                }
            }
            result = self.client.service.deleteAbsenceFilesBetweenTwoDates(**request)

            errors = self._extract_errors(result)

            if not errors:
                logger.info(
                    f"Lot de {len(absence_files)} absence(s) supprimée(s) avec succès"
                )
                return id_map, [], []

            # Match errors back to ID_Demande via comment
            failed_id_set = set()
            error_details = []
            for err in errors:
                comment = err.get("comment", "")
                err_msg = err.get("errorMessage", "Unknown error")
                emp_id = err.get("employeeIdentificationNumber", "?")

                matched_id = self._extract_id_from_comment(comment)
                if matched_id:
                    failed_id_set.add(matched_id)
                    logger.warning(
                        f"Suppression REJETÉE: ID={matched_id}, Matricule={emp_id} — {err_msg}"
                    )
                else:
                    logger.warning(
                        f"Suppression REJETÉE: Matricule={emp_id} — {err_msg}"
                    )

                error_details.append({
                    "ID_Demande": matched_id,
                    "employeeIdentificationNumber": emp_id,
                    "errorMessage": err_msg,
                })

            success_ids = [id_d for id_d in id_map if id_d not in failed_id_set]
            failed_ids = [id_d for id_d in id_map if id_d in failed_id_set]

            logger.info(
                f"Suppression: {len(success_ids)} succès, {len(failed_ids)} rejeté(s)"
            )
            return success_ids, failed_ids, error_details

        except Exception as e:
            logger.error(
                f"Erreur suppression lot de {len(absence_files)} absences: {e}"
            )
            return [], id_map, []

    # ── Build AbsenceFile structure ──────────────────────────────────

    @staticmethod
    def _build_absence_file(record: dict) -> dict:
        """
        Build a Kelio AbsenceFile dict from a GEDRHP record.

        Embeds #ID_Demande at the start of the comment for error matching.

        Mapping:
            Matricule         → employeeIdentificationNumber
            ID_Type_Demande   → absenceTypeAbbreviation
            Date_Debut        → startDate
            IsDateDebutAM     → startInTheMorning
            Date_fin          → endDate
            IsDateFinAM       → endingTheAfternoon
            Date_Creation     → creationDate
            Objet             → comment (prefixed with #ID_Demande)
        """
        # Parse dates
        start_date = record.get("Date_Debut")
        end_date = record.get("Date_fin")
        creation_date = record.get("Date_Creation")

        if isinstance(start_date, datetime):
            start_date = start_date.strftime("%Y-%m-%d")
        else:
            start_date = str(start_date).split(" ")[0] if start_date else None

        if isinstance(end_date, datetime):
            end_date = end_date.strftime("%Y-%m-%d")
        else:
            end_date = str(end_date).split(" ")[0] if end_date else None

        if isinstance(creation_date, datetime):
            creation_date = creation_date.strftime("%Y-%m-%d")
        else:
            creation_date = str(creation_date).split(" ")[0] if creation_date else None

        # Parse booleans
        # IsDateDebutAM: 1 = morning included in leave → startInTheMorning = True
        start_morning = bool(int(record.get("IsDateDebutAM", 1)))
        # IsDateFinAM:   0 = afternoon included in leave → endingTheAfternoon = True (inverted)
        end_afternoon = not bool(int(record.get("IsDateFinAM", 0)))

        # Build comment with #ID_Demande prefix for error matching
        id_demande = str(record.get("ID_Demande", "")).strip()
        objet = str(record.get("Objet", "")).strip()
        comment = f"#{id_demande} {objet}" if id_demande else objet

        logger.debug(
            f"ID={id_demande}: startInTheMorning={start_morning}, endingTheAfternoon={end_afternoon} "
            f"(raw: IsDateDebutAM={record.get('IsDateDebutAM')}, IsDateFinAM={record.get('IsDateFinAM')})"
        )

        absence_file = {
            "employeeIdentificationNumber": str(record.get("Matricule", "")).strip(),
            "absenceTypeAbbreviation": str(record.get("ID_Type_Demande", "")).strip(),
            "startDate": start_date,
            "endDate": end_date,
            "startInTheMorning": start_morning,
            "endingTheAfternoon": end_afternoon,
            "creationDate": creation_date,
            "comment": comment,
        }

        return absence_file

    # ── Extract ID_Demande from comment ──────────────────────────────

    @staticmethod
    def _extract_id_from_comment(comment: str) -> str:
        """
        Extract the ID_Demande from a comment string like '#OM_114024 travail quotidien'.

        Returns:
            str: The ID_Demande (e.g. 'OM_114024') or empty string if not found.
        """
        if not comment or not comment.startswith("#"):
            return ""
        # Split on first space: '#OM_114024 travail quotidien' → 'OM_114024'
        parts = comment.split(" ", 1)
        return parts[0][1:]  # Remove the '#' prefix

    # ── SOAP XML logging ─────────────────────────────────────────────

    def _save_soap_xml(self, filename: str = "soap_absence_response.xml"):
        """Save the last SOAP request/response XML to logs/ directory."""
        try:
            os.makedirs(self._log_dir, exist_ok=True)
            xml_path = os.path.join(self._log_dir, filename)

            with open(xml_path, "w", encoding="utf-8") as f:
                f.write("<!-- ═══ SOAP REQUEST ═══ -->\n")
                try:
                    sent = self.history.last_sent
                    if sent and sent.get("envelope") is not None:
                        f.write(etree.tostring(
                            sent["envelope"],
                            pretty_print=True,
                            encoding="unicode",
                        ))
                except Exception:
                    f.write("<!-- Could not serialize request -->\n")

                f.write("\n\n<!-- ═══ SOAP RESPONSE ═══ -->\n")
                try:
                    received = self.history.last_received
                    if received and received.get("envelope") is not None:
                        f.write(etree.tostring(
                            received["envelope"],
                            pretty_print=True,
                            encoding="unicode",
                        ))
                except Exception:
                    f.write("<!-- Could not serialize response -->\n")

            logger.info(f"XML SOAP sauvegardé dans {xml_path}")
        except Exception as e:
            logger.error(f"Erreur sauvegarde XML SOAP: {e}")

    # ── Error extraction ─────────────────────────────────────────────

    def _extract_errors(self, result) -> list:
        """
        Extract rejected AbsenceFile entries from the SOAP response.

        Returns:
            list[dict]: Error entries with comment, employeeIdentificationNumber,
                        and errorMessage.
        """
        # 1. Try zeep deserialization
        error_list = self._extract_errors_from_zeep(result)
        if error_list:
            return error_list

        # 2. Fallback: parse raw XML
        return self._extract_errors_from_xml()

    @staticmethod
    def _extract_errors_from_zeep(result) -> list:
        """Try to extract error absence files from the zeep-deserialized result."""
        if result is None:
            return []

        logger.debug(f"SOAP Absence response: type={type(result).__name__}, value={result}")

        try:
            from zeep.helpers import serialize_object
            serialized = serialize_object(result, target_cls=dict)
            logger.debug(f"Serialized SOAP Absence response: {serialized}")
        except Exception:
            serialized = None

        errors_raw = None
        if serialized is not None:
            if isinstance(serialized, dict):
                errors_raw = serialized.get("AbsenceFile", [])
            elif isinstance(serialized, list):
                errors_raw = serialized
        else:
            if hasattr(result, "AbsenceFile") and result.AbsenceFile:
                errors_raw = result.AbsenceFile
            elif isinstance(result, list):
                errors_raw = result

        if not errors_raw:
            return []

        if not isinstance(errors_raw, list):
            errors_raw = [errors_raw]

        error_list = []
        for err in errors_raw:
            if err is None:
                continue
            entry = {}
            attrs = (
                "employeeIdentificationNumber", "errorMessage",
                "technicalString", "startDate", "endDate",
                "employeeSurname", "employeeFirstName", "comment",
            )
            if isinstance(err, dict):
                for attr in attrs:
                    val = err.get(attr)
                    if val is not None:
                        entry[attr] = str(val)
            else:
                for attr in attrs:
                    val = getattr(err, attr, None)
                    if val is not None:
                        entry[attr] = str(val)

            if entry.get("employeeIdentificationNumber") or entry.get("errorMessage"):
                error_list.append(entry)

        return error_list

    def _extract_errors_from_xml(self) -> list:
        """Parse the raw SOAP XML response to find rejected absence files."""
        try:
            received = self.history.last_received
            if not received or received.get("envelope") is None:
                return []

            envelope = received["envelope"]
            ns = {"ns": "http://echange.service.open.bodet.com"}

            af_elements = envelope.findall(".//ns:AbsenceFile", ns)
            if not af_elements:
                af_elements = envelope.findall(
                    ".//{http://echange.service.open.bodet.com}AbsenceFile"
                )

            if not af_elements:
                logger.debug("XML fallback: no <AbsenceFile> elements found")
                return []

            error_list = []
            for af in af_elements:
                err_msg_el = (
                    af.find("ns:errorMessage", ns)
                    or af.find("{http://echange.service.open.bodet.com}errorMessage")
                )
                if err_msg_el is None or err_msg_el.text is None:
                    continue

                entry = {"errorMessage": err_msg_el.text}
                for tag in (
                    "employeeIdentificationNumber", "startDate", "endDate",
                    "employeeSurname", "employeeFirstName", "technicalString",
                    "comment",
                ):
                    el = (
                        af.find(f"ns:{tag}", ns)
                        or af.find(f"{{http://echange.service.open.bodet.com}}{tag}")
                    )
                    if el is not None and el.text is not None:
                        entry[tag] = el.text

                if entry.get("employeeIdentificationNumber") or entry.get("errorMessage"):
                    error_list.append(entry)

            if error_list:
                logger.debug(
                    f"XML fallback: found {len(error_list)} rejected absence(s)"
                )
            return error_list

        except Exception as e:
            logger.error(f"Error parsing raw SOAP XML for absence errors: {e}")
            return []
