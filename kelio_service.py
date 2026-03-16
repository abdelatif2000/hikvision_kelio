"""
Kelio SOAP Web Service module - sends attendance records to Kelio.

Reuses the same SOAP pattern (zeep + HTTPBasicAuth + importPhysicalClockings)
from the existing zkteco project.
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


class KelioService:
    """Client for the Bodet Kelio ClockingService SOAP API."""

    def __init__(self, config: Config):
        self.config = config
        self.client = None
        self.history = HistoryPlugin()
        self._log_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "logs"
        )

    def connect(self):
        """
        Establish the SOAP connection to Kelio.

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
                self.config.kelio_soap_url,
                transport=Transport(session=session),
                settings=settings,
                plugins=[self.history],
            )
            logger.info(f"Client SOAP connecté à {self.config.kelio_soap_url}")
            return True

        except Exception as e:
            logger.exception(f"Erreur de connexion au service SOAP Kelio: {e}")
            return False

    def send_clocking(self, record: dict) -> bool:
        """
        Send a single attendance record to Kelio.

        Args:
            record: Dict with keys from the attLog table
                    (employeeID, authDate, authTime, direction, deviceName, …).

        Returns:
            bool: True if the record was sent successfully.
        """
        if not self.client:
            logger.error("Client SOAP non connecté — appelez connect() d'abord")
            return False

        try:
            # Parse date and time from the record
            auth_date = record.get("authDate")
            auth_time = record.get("authTime")

            # Handle cases where authDate/authTime might be datetime objects
            if isinstance(auth_date, datetime):
                date_str = auth_date.strftime("%Y-%m-%d")
            else:
                date_str = str(auth_date)

            if isinstance(auth_time, datetime):
                time_str = auth_time.strftime("%H:%M:%S")
            else:
                time_str = str(auth_time)

            # Build the Clocking structure (same as existing project)
            employee_id = str(record.get("emplyeeID", "")).strip()
            clocking = {
                "employeeIdentificationNumber": employee_id,
                "date": date_str,
                "time": time_str,
                "terminalKey": record.get("deviceName", ""),
            }

            # Call importPhysicalClockings
            request = {
                "clockingsToImport": {
                    "Clocking": [clocking],
                }
            }

            result = self.client.service.importPhysicalClockings(**request)

            # Save the raw XML response for inspection
            self._save_soap_xml()

            # Check if Kelio reported errors for this clocking
            errors_in_response = self._extract_clockings_in_error(result)
            if errors_in_response:
                err_msg = errors_in_response[0].get("errorMessage", "Unknown error")
                logger.warning(
                    f"Pointage REJETÉ par Kelio: employeeID={employee_id}, "
                    f"date={date_str}, time={time_str} — erreur: {err_msg}"
                )
                return False

            logger.debug(
                f"Pointage envoyé: employeeID={employee_id}, "
                f"date={date_str}, time={time_str} — résultat: OK"
            )
            return True

        except Exception as e:
            logger.error(
                f"Erreur envoi pointage serialNo={record.get('serialNo')}: {e}"
            )
            return False

    def send_clockings_batch(self, records: list) -> tuple:
        """
        Send a batch of attendance records to Kelio in a single SOAP call.

        Args:
            records: List of record dicts.

        Returns:
            (success_serials, failed_serials, error_details): 
                success_serials: list of serialNo values sent OK.
                failed_serials: list of serialNo values rejected.
                error_details: list of dicts with 'employeeIdentificationNumber' 
                               and 'errorMessage' for each rejected clocking.
        """
        if not self.client:
            logger.error("Client SOAP non connecté — appelez connect() d'abord")
            return [], [r.get("serialNo") for r in records], []

        # Build all Clocking objects
        clockings = []
        serials = []
        for record in records:
            try:
                auth_date = record.get("authDate")
                auth_time = record.get("authTime")

                if isinstance(auth_date, datetime):
                    date_str = auth_date.strftime("%Y-%m-%d")
                else:
                    date_str = str(auth_date)

                if isinstance(auth_time, datetime):
                    time_str = auth_time.strftime("%H:%M:%S")
                else:
                    time_str = str(auth_time)

                employee_id = str(record.get("emplyeeID", "")).strip()
                clocking = {
                    "employeeIdentificationNumber": employee_id,
                    "date": date_str,
                    "time": time_str,
                    "terminalKey": record.get("deviceName", ""),
                }
                clockings.append(clocking)
                serials.append(record.get("serialNo"))

            except Exception as e:
                logger.error(
                    f"Erreur préparation pointage serialNo={record.get('serialNo')}: {e}"
                )

        if not clockings:
            return [], [r.get("serialNo") for r in records], []

        # Send all clockings in a single SOAP call
        try:
            request = {
                "clockingsToImport": {
                    "Clocking": clockings,
                }
            }
            result = self.client.service.importPhysicalClockings(**request)

            # Save the raw XML response to a file for inspection
            self._save_soap_xml()

            # Parse the response to find which clockings were rejected
            errors_in_response = self._extract_clockings_in_error(result)

            if not errors_in_response:
                # All clockings succeeded
                logger.info(
                    f"Lot de {len(clockings)} pointage(s) envoyé avec succès"
                )
                return serials, [], []

            # Build a set of employee IDs that Kelio rejected
            failed_employee_ids = set()
            error_details = []
            for err_clocking in errors_in_response:
                emp_id = err_clocking.get("employeeIdentificationNumber", "")
                err_msg = err_clocking.get("errorMessage", "Unknown error")
                failed_employee_ids.add(str(emp_id).strip())
                error_details.append({
                    "employeeIdentificationNumber": emp_id,
                    "errorMessage": err_msg,
                })

            # Split serials into success / fail based on the error response
            success_serials = []
            failed_serials = []
            for clocking_obj, serial in zip(clockings, serials):
                emp_id = str(clocking_obj.get("employeeIdentificationNumber", "")).strip()
                if emp_id in failed_employee_ids:
                    failed_serials.append(serial)
                else:
                    success_serials.append(serial)

            logger.info(
                f"Lot de {len(clockings)} pointage(s): "
                f"{len(success_serials)} succès, {len(failed_serials)} rejeté(s)"
            )
            return success_serials, failed_serials, error_details

        except Exception as e:
            logger.error(f"Erreur envoi lot de {len(clockings)} pointages: {e}")
            return [], serials, []

    def _save_soap_xml(self):
        """Save the last SOAP request/response XML to logs/soap_response.xml."""
        try:
            os.makedirs(self._log_dir, exist_ok=True)
            xml_path = os.path.join(self._log_dir, "soap_response.xml")

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

    def _extract_clockings_in_error(self, result) -> list:
        """
        Extract the list of rejected clockings from the Kelio SOAP response.

        The `importPhysicalClockingsResponse` returns a `clockingsInError`
        field containing an ArrayOfClocking with error details.

        Uses zeep deserialization first, then falls back to raw XML parsing
        from self.history if zeep yields no errors.

        Returns:
            list[dict]: List of error clocking dicts, each with keys like
                        'employeeIdentificationNumber' and 'errorMessage'.
                        Empty list if no errors.
        """
        # ── 1. Try zeep deserialization ───────────────────────────────
        error_list = self._extract_errors_from_zeep(result)
        if error_list:
            return error_list

        # ── 2. Fallback: parse the raw XML from history ──────────────
        error_list = self._extract_errors_from_xml()
        return error_list

    # ── helper: zeep object deserialization ──────────────────────────
    @staticmethod
    def _extract_errors_from_zeep(result) -> list:
        """Try to extract error clockings from the zeep-deserialized result."""
        if result is None:
            return []

        logger.debug(f"SOAP response: type={type(result).__name__}, value={result}")

        # Use zeep's serialize_object for reliable conversion
        try:
            from zeep.helpers import serialize_object
            serialized = serialize_object(result, target_cls=dict)
            logger.debug(f"Serialized SOAP response: {serialized}")
        except Exception:
            serialized = None

        # Extract the list of Clocking objects from the response
        errors_raw = None

        if serialized is not None:
            if isinstance(serialized, dict):
                errors_raw = serialized.get("Clocking", [])
            elif isinstance(serialized, list):
                errors_raw = serialized
        else:
            if hasattr(result, 'Clocking') and result.Clocking:
                errors_raw = result.Clocking
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
            if isinstance(err, dict):
                for attr in ('employeeIdentificationNumber', 'errorMessage',
                             'technicalString', 'date', 'time',
                             'employeeSurname', 'employeeFirstName'):
                    val = err.get(attr)
                    if val is not None:
                        entry[attr] = str(val)
            else:
                for attr in ('employeeIdentificationNumber', 'errorMessage',
                             'technicalString', 'date', 'time',
                             'employeeSurname', 'employeeFirstName'):
                    val = getattr(err, attr, None)
                    if val is not None:
                        entry[attr] = str(val)

            if entry.get('employeeIdentificationNumber') or entry.get('errorMessage'):
                error_list.append(entry)

        return error_list

    # ── helper: raw XML fallback parsing ─────────────────────────────
    def _extract_errors_from_xml(self) -> list:
        """Parse the raw SOAP XML response to find rejected clockings."""
        try:
            received = self.history.last_received
            if not received or received.get("envelope") is None:
                return []

            envelope = received["envelope"]
            # Search for all <Clocking> elements that contain an <errorMessage>
            # Use a namespace-agnostic search to handle any namespace prefix
            ns = {"ns": "http://echange.service.open.bodet.com"}

            clocking_elements = envelope.findall(".//ns:Clocking", ns)
            if not clocking_elements:
                # Try namespace-agnostic search as last resort
                clocking_elements = envelope.findall(
                    ".//{http://echange.service.open.bodet.com}Clocking"
                )

            if not clocking_elements:
                logger.debug("XML fallback: no <Clocking> elements found")
                return []

            error_list = []
            for clk in clocking_elements:
                # Extract text from child elements (namespace-aware)
                err_msg_el = (
                    clk.find("ns:errorMessage", ns)
                    or clk.find("{http://echange.service.open.bodet.com}errorMessage")
                )
                if err_msg_el is None or err_msg_el.text is None:
                    continue  # No error message → not a rejected clocking

                entry = {"errorMessage": err_msg_el.text}

                # Extract other useful fields
                for tag in ('employeeIdentificationNumber', 'date', 'time',
                            'employeeSurname', 'employeeFirstName',
                            'technicalString'):
                    el = (
                        clk.find(f"ns:{tag}", ns)
                        or clk.find(f"{{http://echange.service.open.bodet.com}}{tag}")
                    )
                    if el is not None and el.text is not None:
                        entry[tag] = el.text

                if entry.get('employeeIdentificationNumber') or entry.get('errorMessage'):
                    error_list.append(entry)

            if error_list:
                logger.debug(
                    f"XML fallback: found {len(error_list)} rejected clocking(s)"
                )
            return error_list

        except Exception as e:
            logger.error(f"Error parsing raw SOAP XML for errors: {e}")
            return []
