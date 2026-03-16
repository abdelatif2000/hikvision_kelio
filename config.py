"""
Configuration module - loads settings from .env file.
"""

import os
from dataclasses import dataclass
from datetime import date, timedelta
from dotenv import load_dotenv

# Load .env from the same directory as this file
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_env_path)


@dataclass
class Config:
    """Application configuration loaded from environment variables."""

    # SQL Server (HikVision attendance)
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_user: str = os.getenv("DB_USER", "sa")
    db_password: str = os.getenv("DB_PASSWORD", "manager")
    db_name: str = os.getenv("DB_NAME", "HikVision")
    db_table: str = os.getenv("DB_TABLE", "attLog")
    db_driver: str = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

    # SQL Server (Absence - GEDRHP table)
    absence_db_host: str = os.getenv("ABSENCE_DB_HOST", "localhost")
    absence_db_user: str = os.getenv("ABSENCE_DB_USER", "sa")
    absence_db_password: str = os.getenv("ABSENCE_DB_PASSWORD", "manager")
    absence_db_name: str = os.getenv("ABSENCE_DB_NAME", "Interface_GED_KELIO_2")
    absence_db_driver: str = os.getenv("ABSENCE_DB_DRIVER", "ODBC Driver 17 for SQL Server")
    absence_db_table: str = os.getenv("ABSENCE_DB_TABLE", "GEDRHP")
    absence_months_back: int = int(os.getenv("ABSENCE_MONTHS_BACK", "3"))
    # Comma-separated Code_Entite values (empty = all)
    absence_code_entite: str = os.getenv("ABSENCE_CODE_ENTITE", "")

    # SQL Server (Employee Sync DB — local, with write access)
    employee_sync_db_host: str = os.getenv("EMPLOYEE_SYNC_DB_HOST", "localhost")
    employee_sync_db_user: str = os.getenv("EMPLOYEE_SYNC_DB_USER", "sa")
    employee_sync_db_password: str = os.getenv("EMPLOYEE_SYNC_DB_PASSWORD", "manager")
    employee_sync_db_name: str = os.getenv("EMPLOYEE_SYNC_DB_NAME", "Sync_interface_GED_KELIO")
    employee_sync_db_driver: str = os.getenv("EMPLOYEE_SYNC_DB_DRIVER", "ODBC Driver 17 for SQL Server")
    employee_sync_table: str = os.getenv("EMPLOYEE_SYNC_TABLE", "EmployeeSync")

    # Employee Source (Collaborateur via Linked Server)
    employee_source_linked_server: str = os.getenv("EMPLOYEE_SOURCE_LINKED_SERVER", "SOURCE_SERVER_GEDRHP")
    employee_source_db: str = os.getenv("EMPLOYEE_SOURCE_DB", "Interface_GED_KELIO")
    employee_source_table: str = os.getenv("EMPLOYEE_SOURCE_TABLE", "Collaborateur")
    employee_default_section: str = os.getenv("EMPLOYEE_DEFAULT_SECTION", "ZTT")
    # Optional: filter employees by Date_Embauche >= this value. Empty = all.
    employee_date_embauche_from: str = os.getenv("EMPLOYEE_DATE_EMBAUCHE_FROM", "")
    # Comma-separated Entite_Juridique values to filter with LIKE (empty = all)
    employee_entite_juridique: str = os.getenv("EMPLOYEE_ENTITE_JURIDIQUE", "")

    # Kelio SOAP (Clocking)
    kelio_soap_url: str = os.getenv("KELIO_SOAP_URL", "")
    kelio_soap_login: str = os.getenv("KELIO_SOAP_LOGIN", "")
    kelio_soap_password: str = os.getenv("KELIO_SOAP_PASSWORD", "")

    # Kelio SOAP (Absence)
    kelio_absence_soap_url: str = os.getenv("KELIO_ABSENCE_SOAP_URL", "")

    # Kelio SOAP (Employee)
    kelio_employee_soap_url: str = os.getenv("KELIO_EMPLOYEE_SOAP_URL", "")
    kelio_employee_field_soap_url: str = os.getenv("KELIO_EMPLOYEE_FIELD_SOAP_URL", "")

    # Processing
    batch_size: int = int(os.getenv("BATCH_SIZE", "100"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # SMTP Email Alerts
    smtp_host: str = os.getenv("SMTP_HOST", "192.168.1.56")
    smtp_port: int = int(os.getenv("SMTP_PORT", "25"))
    smtp_from: str = os.getenv("SMTP_FROM", "kelio@synergiesprogres.ma")
    smtp_to: str = os.getenv("SMTP_TO", "kelio@synergiesprogres.ma")
    smtp_user: str = os.getenv("SMTP_USER", "")
    smtp_password: str = os.getenv("SMTP_PASSWORD", "")
    smtp_enabled: bool = os.getenv("SMTP_ENABLED", "true").lower() in ("true", "1", "yes")

    @property
    def absence_date_from(self) -> str:
        """Compute the start date as today minus absence_months_back months."""
        today = date.today()
        # Subtract months: go back N months
        month = today.month - self.absence_months_back
        year = today.year
        while month <= 0:
            month += 12
            year -= 1
        # Clamp day to max days in target month
        import calendar
        max_day = calendar.monthrange(year, month)[1]
        day = min(today.day, max_day)
        return date(year, month, day).strftime("%Y-%m-%d")

    @property
    def db_connection_string(self) -> str:
        """Build the pyodbc connection string for HikVision DB."""
        return (
            f"DRIVER={{{self.db_driver}}};"
            f"SERVER={self.db_host};"
            f"DATABASE={self.db_name};"
            f"UID={self.db_user};"
            f"PWD={self.db_password};"
            f"TrustServerCertificate=yes;"
        )

    @property
    def absence_db_connection_string(self) -> str:
        """Build the pyodbc connection string for Interface_GED_KELIO DB."""
        return (
            f"DRIVER={{{self.absence_db_driver}}};"
            f"SERVER={self.absence_db_host};"
            f"DATABASE={self.absence_db_name};"
            f"UID={self.absence_db_user};"
            f"PWD={self.absence_db_password};"
            f"TrustServerCertificate=yes;"
        )

    @property
    def employee_sync_db_connection_string(self) -> str:
        """Build the pyodbc connection string for the Employee Sync DB."""
        return (
            f"DRIVER={{{self.employee_sync_db_driver}}};"
            f"SERVER={self.employee_sync_db_host};"
            f"DATABASE={self.employee_sync_db_name};"
            f"UID={self.employee_sync_db_user};"
            f"PWD={self.employee_sync_db_password};"
            f"TrustServerCertificate=yes;"
        )

    @property
    def employee_source_full_table(self) -> str:
        """Full linked-server path to the Collaborateur table."""
        return (
            f"[{self.employee_source_linked_server}]"
            f".[{self.employee_source_db}]"
            f".[dbo]"
            f".[{self.employee_source_table}]"
        )

    def validate(self):
        """Validate that all required settings are present."""
        errors = []
        if not self.db_host:
            errors.append("DB_HOST is required")
        if not self.db_name:
            errors.append("DB_NAME is required")
        if not self.kelio_soap_url:
            errors.append("KELIO_SOAP_URL is required")
        if errors:
            raise ValueError(
                "Configuration errors:\n  - " + "\n  - ".join(errors)
            )

    def validate_absence(self):
        """Validate absence-specific settings."""
        errors = []
        if not self.absence_db_host:
            errors.append("ABSENCE_DB_HOST is required")
        if not self.absence_db_name:
            errors.append("ABSENCE_DB_NAME is required")
        if not self.absence_db_table:
            errors.append("ABSENCE_DB_TABLE is required")
        if not self.kelio_absence_soap_url:
            errors.append("KELIO_ABSENCE_SOAP_URL is required")
        if errors:
            raise ValueError(
                "Absence configuration errors:\n  - " + "\n  - ".join(errors)
            )

    def validate_employee(self):
        """Validate employee-specific settings."""
        errors = []
        if not self.employee_sync_db_host:
            errors.append("EMPLOYEE_SYNC_DB_HOST is required")
        if not self.employee_sync_db_name:
            errors.append("EMPLOYEE_SYNC_DB_NAME is required")
        if not self.employee_source_linked_server:
            errors.append("EMPLOYEE_SOURCE_LINKED_SERVER is required")
        if not self.employee_source_db:
            errors.append("EMPLOYEE_SOURCE_DB is required")
        if not self.employee_source_table:
            errors.append("EMPLOYEE_SOURCE_TABLE is required")
        if not self.kelio_employee_soap_url:
            errors.append("KELIO_EMPLOYEE_SOAP_URL is required")
        if not self.kelio_employee_field_soap_url:
            errors.append("KELIO_EMPLOYEE_FIELD_SOAP_URL is required")
        if errors:
            raise ValueError(
                "Employee configuration errors:\n  - " + "\n  - ".join(errors)
            )


def get_config() -> Config:
    """Create and validate a Config instance."""
    config = Config()
    config.validate()
    return config


def get_absence_config() -> Config:
    """Create and validate a Config instance for absence import."""
    config = Config()
    config.validate_absence()
    return config


def get_employee_config() -> Config:
    """Create and validate a Config instance for employee import."""
    config = Config()
    config.validate_employee()
    return config


