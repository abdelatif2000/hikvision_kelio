"""
Email alert module — sends error summaries via SMTP.
Uses Python built-in smtplib (no extra dependencies).

Includes digest/deduplication: tracks errors by their identifier (same one
used in the import) and only sends an email when NEW or UPDATED errors appear.
If all current errors were already reported, the email is skipped.
"""

import hashlib
import json
import logging
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger("hikvision_kelio")

# Directory where dedup state files are stored (next to the script)
_STATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")


def _get_identifier(err: dict) -> str:
    """Extract the identifier from an error dict, using the same keys as the import."""
    return (
        err.get("employeeIdentificationNumber")
        or err.get("MatriculePointeuse")
        or err.get("ID_Demande")
        or "?"
    )


def _state_file_path(subject: str) -> str:
    """Return the path to the JSON state file for a given alert subject."""
    safe = "".join(c if c.isalnum() else "_" for c in subject).strip("_")
    return os.path.join(_STATE_DIR, f".email_state_{safe}.json")


def _load_previous_state(subject: str) -> dict:
    """Load the previous error state {identifier: errorMessage}."""
    path = _state_file_path(subject)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("errors", {})
    except Exception:
        return {}


def _save_state(subject: str, current_errors: dict):
    """Persist the current errors so the next run can compare."""
    os.makedirs(_STATE_DIR, exist_ok=True)
    path = _state_file_path(subject)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {"errors": current_errors, "last_sent": datetime.now().isoformat()},
                f,
                ensure_ascii=False,
            )
    except Exception as e:
        logger.warning(f"Impossible de sauvegarder l'état email: {e}")


def _has_new_or_updated(error_details: list[dict], previous: dict) -> bool:
    """Return True if at least one error is new or has a changed message."""
    for err in error_details:
        identifier = _get_identifier(err)
        message = err.get("errorMessage", "")
        prev_message = previous.get(identifier)
        if prev_message is None or prev_message != message:
            # New identifier or updated message
            return True
    return False


def send_error_alert(subject: str, error_details: list[dict], config) -> bool:
    """
    Send an HTML email with a table of import errors.

    Deduplication: compares current errors (by identifier) against the last
    emailed state. Only sends if there is at least one NEW or UPDATED error.
    The email table shows only error details (no status column).

    Parameters
    ----------
    subject : str
        Email subject line.
    error_details : list[dict]
        Each dict should have at least an "errorMessage" key and an identifier
        key (e.g. "employeeIdentificationNumber" or "MatriculePointeuse").
    config : Config
        Application configuration with SMTP settings.

    Returns
    -------
    bool
        True if the email was sent successfully, False otherwise.
    """
    if not config.smtp_enabled:
        logger.info("Alertes email désactivées (SMTP_ENABLED=false).")
        return False

    if not error_details:
        logger.debug("Aucune erreur à envoyer par email.")
        return False

    if not config.smtp_to:
        logger.warning("SMTP_TO non configuré — email non envoyé.")
        return False

    # ── Filter out ignored errors ──────────────────────────────────
    ignored_messages = ["absence déjà existante"]
    filtered_errors = []
    for err in error_details:
        msg_lower = err.get("errorMessage", "").lower()
        if not any(ignored in msg_lower for ignored in ignored_messages):
            filtered_errors.append(err)

    if not filtered_errors:
        logger.debug("Toutes les erreurs ont été ignorées (ex: Absence déjà existante). Email annulé.")
        return False

    # ── Deduplication check ────────────────────────────────────────
    previous_state = _load_previous_state(subject)

    if previous_state and not _has_new_or_updated(filtered_errors, previous_state):
        logger.info(
            f"Email non envoyé — toutes les erreurs ont déjà été signalées. "
            f"Sujet: {subject}"
        )
        return False

    # ── Build HTML body ────────────────────────────────────────────
    rows_html = ""
    for i, err in enumerate(filtered_errors, 1):
        identifier = _get_identifier(err)
        message = err.get("errorMessage", "Erreur inconnue")
        rows_html += (
            f"<tr>"
            f"<td style='padding:6px 12px;border:1px solid #ddd;text-align:center'>{i}</td>"
            f"<td style='padding:6px 12px;border:1px solid #ddd'>{identifier}</td>"
            f"<td style='padding:6px 12px;border:1px solid #ddd'>{message}</td>"
            f"</tr>\n"
        )

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html = f"""\
<html>
<body style="font-family:Segoe UI,Arial,sans-serif;color:#333">
    <h2 style="color:#c0392b">⚠ {subject}</h2>
    <p>Date/Heure : <strong>{timestamp}</strong></p>
    <p>Nombre d'erreurs : <strong>{len(filtered_errors)}</strong></p>
    <table style="border-collapse:collapse;width:100%;margin-top:12px">
        <thead>
            <tr style="background:#c0392b;color:#fff">
                <th style="padding:8px 12px;border:1px solid #ddd">#</th>
                <th style="padding:8px 12px;border:1px solid #ddd">Identifiant</th>
                <th style="padding:8px 12px;border:1px solid #ddd">Message d'erreur</th>
            </tr>
        </thead>
        <tbody>
            {rows_html}
        </tbody>
    </table>
    <p style="margin-top:20px;font-size:12px;color:#888">
        Ce message a été envoyé automatiquement par le système d'import Kelio.
    </p>
</body>
</html>
"""

    # ── Build and send email ───────────────────────────────────────
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config.smtp_from
    msg["To"] = config.smtp_to
    msg.attach(MIMEText(html, "html", "utf-8"))

    recipients = [addr.strip() for addr in config.smtp_to.split(",")]

    try:
        with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=30) as server:
            if config.smtp_user and config.smtp_password:
                server.starttls()
                server.login(config.smtp_user, config.smtp_password)
            server.sendmail(config.smtp_from, recipients, msg.as_string())
        logger.info(
            f"Email d'alerte envoyé à {config.smtp_to} ({len(filtered_errors)} erreur(s))."
        )
        # Save state AFTER successful send
        current_state = {
            _get_identifier(err): err.get("errorMessage", "")
            for err in filtered_errors
        }
        _save_state(subject, current_state)
        return True
    except Exception as e:
        logger.error(f"Échec de l'envoi de l'email d'alerte: {e}")
        return False
