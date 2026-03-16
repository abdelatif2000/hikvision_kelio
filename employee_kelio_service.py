"""
Kelio Employee SOAP module — imports employees into Kelio.

Uses two SOAP services:
- EmployeeService (importEmployees) for core employee data
- EmployeeFieldAssignmentService (importEmployeeFreeFieldAssignments) for the Bp free field

Follows the same zeep + HTTPBasicAuth pattern as absence_kelio_service.py.
Sends records in bulk. Skips any NULL DB column (not sent to Kelio).
"""

import logging
from datetime import datetime, date

from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client, Settings
from zeep.transports import Transport
from zeep.plugins import HistoryPlugin

from config import Config

logger = logging.getLogger("hikvision_kelio")


class EmployeeKelioService:
    """Client for the Bodet Kelio EmployeeService & EmployeeFieldAssignmentService SOAP APIs."""

    def __init__(self, config: Config):
        self.config = config
        self.employee_client = None
        self.field_client = None
        self.employee_history = HistoryPlugin()
        self.field_history = HistoryPlugin()

    # ── Connection ───────────────────────────────────────────────────

    def connect(self) -> bool:
        """
        Establish SOAP connections to EmployeeService and EmployeeFieldService.

        Returns:
            bool: True if both connections succeeded.
        """
        try:
            session = Session()
            if self.config.kelio_soap_login and self.config.kelio_soap_password:
                session.auth = HTTPBasicAuth(
                    self.config.kelio_soap_login,
                    self.config.kelio_soap_password,
                )

            settings = Settings(strict=False)

            # EmployeeService client
            self.employee_client = Client(
                self.config.kelio_employee_soap_url,
                transport=Transport(session=session),
                settings=settings,
                plugins=[self.employee_history],
            )
            logger.info(
                f"Client SOAP Employee connecté à {self.config.kelio_employee_soap_url}"
            )

            # EmployeeFieldAssignmentService client (needs its own session for history)
            field_session = Session()
            if self.config.kelio_soap_login and self.config.kelio_soap_password:
                field_session.auth = HTTPBasicAuth(
                    self.config.kelio_soap_login,
                    self.config.kelio_soap_password,
                )

            self.field_client = Client(
                self.config.kelio_employee_field_soap_url,
                transport=Transport(session=field_session),
                settings=settings,
                plugins=[self.field_history],
            )
            logger.info(
                f"Client SOAP EmployeeFieldAssignment connecté à {self.config.kelio_employee_field_soap_url}"
            )

            return True

        except Exception as e:
            logger.exception(
                f"Erreur de connexion aux services SOAP Employee Kelio: {e}"
            )
            return False

    # ── Import employees (bulk) ──────────────────────────────────────

    def import_employees(self, records: list) -> tuple:
        """
        Import employee records into Kelio in a single bulk SOAP call.

        Args:
            records: List of record dicts from the Collaborateur table.

        Returns:
            (success_matricules, failed_matricules, error_details):
                success_matricules: list of MatriculePointeuse values imported OK.
                failed_matricules: list of MatriculePointeuse values rejected.
                error_details: list of error dicts with details.
        """
        if not self.employee_client:
            logger.error("Client SOAP Employee non connecté — appelez connect() d'abord")
            return [], [r.get("MatriculePointeuse") for r in records], []

        # Build Employee objects
        employees = []
        matricule_map = []
        for record in records:
            try:
                employee = self._build_employee(record)
                if employee:
                    employees.append(employee)
                    matricule_map.append(str(record.get("MatriculePointeuse", "")).strip())
            except Exception as e:
                mat = record.get("MatriculePointeuse", "?")
                logger.error(f"Erreur préparation employé MatriculePointeuse={mat}: {e}")

        if not employees:
            return [], [str(r.get("MatriculePointeuse", "")).strip() for r in records], []

        # Send bulk SOAP call
        try:
            request = {
                "employeesToImport": {
                    "Employee": employees,
                }
            }
            result = self.employee_client.service.importEmployees(**request)

            # Extract errors
            errors = self._extract_employee_errors(result)

            if not errors:
                logger.info(
                    f"Lot de {len(employees)} employé(s) importé(s) avec succès"
                )
                return matricule_map, [], []

            # Match errors to matricules
            failed_set = set()
            error_details = []
            for err in errors:
                emp_id = err.get("identificationNumber", "?")
                err_msg = err.get("errorMessage", "Unknown error")
                failed_set.add(str(emp_id).strip())
                logger.warning(
                    f"Employé REJETÉ: MatriculePointeuse={emp_id} — {err_msg}"
                )
                error_details.append({
                    "MatriculePointeuse": emp_id,
                    "errorMessage": err_msg,
                })

            success = [m for m in matricule_map if m not in failed_set]
            failed = [m for m in matricule_map if m in failed_set]

            logger.info(
                f"Import employés: {len(success)} succès, {len(failed)} rejeté(s)"
            )
            return success, failed, error_details

        except Exception as e:
            logger.error(f"Erreur envoi lot de {len(employees)} employés: {e}")
            return [], matricule_map, []

    # ── Import employee free fields (bulk) ───────────────────────────

    def import_employee_fields(self, records: list) -> tuple:
        """
        Import the Bp free field assignment for employees.

        Uses EmployeeFieldAssignmentService.importEmployeeFreeFieldAssignments.
        Each EmployeeFieldAssignment identifies the employee via employeeIdentificationNumber
        and sets the field abbreviation + value.

        Args:
            records: List of record dicts from the Collaborateur table.

        Returns:
            (success_matricules, failed_matricules, error_details)
        """
        if not self.field_client:
            logger.error("Client SOAP EmployeeFieldAssignment non connecté — appelez connect() d'abord")
            return [], [r.get("MatriculePointeuse") for r in records], []

        # Build EmployeeFieldAssignment objects
        assignments = []
        matricule_map = []
        for record in records:
            matricule = record.get("MatriculePointeuse")
            if matricule is None:
                continue

            matricule_paie = record.get("MatriculePaie")
            if matricule_paie is None:
                continue

            matricule_str = str(matricule).strip()
            matricule_paie_str = str(matricule_paie).strip()
            if not matricule_str or not matricule_paie_str:
                continue

            assignment = {
                "employeeIdentificationNumber": matricule_str,
                "employeeFieldDataAbbreviation": "Bp",
                "employeeFieldValue": matricule_paie_str,
            }
            assignments.append(assignment)
            matricule_map.append(matricule_str)

        if not assignments:
            logger.info("Aucun champ libre Bp à importer.")
            return [], [], []

        # Send bulk SOAP call
        try:
            request = {
                "employeeFreeFieldAssignmentsToImport": {
                    "EmployeeFieldAssignment": assignments,
                }
            }
            result = self.field_client.service.importEmployeeFreeFieldAssignments(**request)

            # Extract errors
            errors = self._extract_field_errors(result)

            if not errors:
                logger.info(
                    f"Lot de {len(assignments)} champ(s) Bp importé(s) avec succès"
                )
                return matricule_map, [], []

            # Match errors by employeeIdentificationNumber
            failed_set = set()
            error_details = []
            for err in errors:
                emp_id = err.get("employeeIdentificationNumber", "?")
                err_msg = err.get("errorMessage", "Unknown error")
                failed_set.add(str(emp_id).strip())
                logger.warning(
                    f"Champ libre REJETÉ: MatriculePointeuse={emp_id} — {err_msg}"
                )
                error_details.append({
                    "MatriculePointeuse": emp_id,
                    "errorMessage": err_msg,
                })

            success = [m for m in matricule_map if m not in failed_set]
            failed = [m for m in matricule_map if m in failed_set]

            logger.info(
                f"Import champs Bp: {len(success)} succès, {len(failed)} rejeté(s)"
            )
            return success, failed, error_details

        except Exception as e:
            logger.error(f"Erreur envoi lot de {len(assignments)} champs Bp: {e}")
            return [], matricule_map, []

    # ── Build Employee structure ─────────────────────────────────────

    def _build_employee(self, record: dict) -> dict | None:
        """
        Build a Kelio Employee dict from a Collaborateur record.

        Skips any field whose DB value is NULL.
        Returns None if MatriculePointeuse is NULL (cannot identify the employee).

        Mapping:
            MatriculePointeuse → identificationNumber
            Nom                → surname
            Prenom             → firstName
            Date_Embauche      → arrivalInCompanyDate
            Date_Naissance     → birthDate
            TypeDeContrat      → currentTimeContractDescription
            Date_Départ        → takenIntoAccountEndDate
            (hardcoded)        → currentSectionAbbreviation (from config)
        """
        matricule = record.get("MatriculePointeuse")
        if matricule is None:
            logger.warning("Enregistrement ignoré: MatriculePointeuse est NULL")
            return None

        employee = {
            "identificationNumber": str(matricule).strip(),
            "useDefaultModelEmployee": True,
        }

        # Nom → surname
        nom = record.get("Nom")
        if nom is not None:
            employee["surname"] = str(nom).strip()

        # Prenom → firstName
        prenom = record.get("Prenom")
        if prenom is not None:
            employee["firstName"] = str(prenom).strip()

        # Date_Embauche → arrivalInCompanyDate + takenIntoAccountPeriodStartDate
        date_embauche = record.get("Date_Embauche")
        if date_embauche is not None:
            formatted = self._format_date(date_embauche)
            employee["arrivalInCompanyDate"] = formatted
            employee["takenIntoAccountPeriodStartDate"] = formatted

        # Date_Naissance → birthDate
        date_naissance = record.get("Date_Naissance")
        if date_naissance is not None:
            employee["birthDate"] = self._format_date(date_naissance)

        # TypeDeContrat → currentTimeContractDescription
        type_contrat = record.get("TypeDeContrat")
        if type_contrat is not None:
            employee["currentTimeContractDescription"] = str(type_contrat).strip()

        # Date_Départ → takenIntoAccountEndDate
        date_depart = record.get("Date_Départ")
        if date_depart is None:
            date_depart = record.get("Date_Depart")  # fallback for encoding
        if date_depart is not None:
            employee["takenIntoAccountPeriodEndDate"] = self._format_date(date_depart)

        # Default section abbreviation (applied to all employees)
        # if self.config.employee_default_section:
        #     employee["currentSectionAbbreviation"] = self.config.employee_default_section

        logger.debug(
            f"Employé préparé: MatriculePointeuse={employee['identificationNumber']}, "
            f"fields={list(employee.keys())}"
        )

        return employee

    # ── Date formatting helper ───────────────────────────────────────

    @staticmethod
    def _format_date(value) -> str:
        """Format a date value to YYYY-MM-DD string."""
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        elif isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        else:
            # Try to extract date part from string
            return str(value).split(" ")[0] if value else ""

    # ── Error extraction (EmployeeService) ───────────────────────────

    def _extract_employee_errors(self, result) -> list:
        """Extract rejected Employee entries from the SOAP response."""
        # 1. Try zeep deserialization
        error_list = self._extract_errors_from_zeep(result, "Employee")
        if error_list:
            return error_list

        # 2. Fallback: parse raw XML
        return self._extract_errors_from_xml(
            self.employee_history, "Employee",
            id_field="identificationNumber"
        )

    def _extract_field_errors(self, result) -> list:
        """Extract rejected EmployeeFieldAssignment entries from the SOAP response."""
        error_list = self._extract_errors_from_zeep(result, "EmployeeFieldAssignment")
        if error_list:
            return error_list

        return self._extract_errors_from_xml(
            self.field_history, "EmployeeFieldAssignment",
            id_field="employeeIdentificationNumber"
        )

    # ── Generic zeep error extraction ────────────────────────────────

    @staticmethod
    def _extract_errors_from_zeep(result, element_name: str) -> list:
        """Try to extract error entries from the zeep-deserialized result."""
        if result is None:
            return []

        logger.debug(f"SOAP Employee response: type={type(result).__name__}, value={result}")

        try:
            from zeep.helpers import serialize_object
            serialized = serialize_object(result, target_cls=dict)
            logger.debug(f"Serialized SOAP Employee response: {serialized}")
        except Exception:
            serialized = None

        errors_raw = None
        if serialized is not None:
            if isinstance(serialized, dict):
                errors_raw = serialized.get(element_name, [])
            elif isinstance(serialized, list):
                errors_raw = serialized
        else:
            if hasattr(result, element_name) and getattr(result, element_name):
                errors_raw = getattr(result, element_name)
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
            # Collect all available attributes
            attrs = (
                "identificationNumber", "employeeIdentificationNumber",
                "surname", "firstName", "errorMessage", "technicalString",
                "employeeFieldAbbreviation", "employeeFieldValue",
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

            if entry.get("errorMessage"):
                error_list.append(entry)

        return error_list

    # ── Generic XML fallback error extraction ────────────────────────

    @staticmethod
    def _extract_errors_from_xml(history, element_name: str, id_field: str) -> list:
        """Parse the raw SOAP XML response to find rejected entries."""
        try:
            received = history.last_received
            if not received or received.get("envelope") is None:
                return []

            envelope = received["envelope"]
            ns = {"ns": "http://echange.service.open.bodet.com"}

            elements = envelope.findall(f".//ns:{element_name}", ns)
            if not elements:
                elements = envelope.findall(
                    f".//{{http://echange.service.open.bodet.com}}{element_name}"
                )

            if not elements:
                logger.debug(f"XML fallback: no <{element_name}> elements found")
                return []

            error_list = []
            for el in elements:
                err_msg_el = el.find("ns:errorMessage", ns)
                if err_msg_el is None:
                    err_msg_el = el.find("{http://echange.service.open.bodet.com}errorMessage")
                if err_msg_el is None or err_msg_el.text is None:
                    continue

                entry = {"errorMessage": err_msg_el.text}
                for tag in (id_field, "surname", "firstName", "technicalString",
                            "employeeFieldAbbreviation", "employeeFieldValue"):
                    tag_el = el.find(f"ns:{tag}", ns)
                    if tag_el is None:
                        tag_el = el.find(f"{{http://echange.service.open.bodet.com}}{tag}")
                    if tag_el is not None and tag_el.text is not None:
                        entry[tag] = tag_el.text

                if entry.get("errorMessage"):
                    error_list.append(entry)

            if error_list:
                logger.debug(
                    f"XML fallback: found {len(error_list)} rejected {element_name}(s)"
                )
            return error_list

        except Exception as e:
            logger.error(f"Error parsing raw SOAP XML for {element_name} errors: {e}")
            return []
