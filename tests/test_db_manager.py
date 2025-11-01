import pytest
from src.db_manager import DBManager

def test_database_connection(db_manager):
    """Test successful database connection"""
    assert db_manager.conn is not None

def test_database_disconnect(db_manager):
    """Test database disconnect functionality"""
    db_manager.disconnect()
    assert db_manager.conn is None