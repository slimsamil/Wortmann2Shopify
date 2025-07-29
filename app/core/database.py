import pyodbc
from contextlib import contextmanager
from typing import Generator
from app.core.config import settings


class DatabaseManager:
    def __init__(self):
        self.connection_string = (
            f"DRIVER={{{settings.db_driver}}};"
            f"SERVER={settings.db_server};"
            f"DATABASE={settings.db_name};"
            f"UID={settings.db_user};"
            f"PWD={settings.db_password};"
            f"TrustServerCertificate=yes;"
        )
    
    @contextmanager
    def get_connection(self) -> Generator[pyodbc.Connection, None, None]:
        """Get database connection with automatic cleanup"""
        connection = None
        try:
            connection = pyodbc.connect(self.connection_string)
            yield connection
        finally:
            if connection:
                connection.close()
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                return True
        except Exception:
            return False


db_manager = DatabaseManager()
