import pytest
import pandas as pd
import sqlite3
from unittest.mock import Mock
from src.db_manager import DBManager

@pytest.fixture(scope="session")
def test_db():
    """Create test database connection"""
    return ":memory:"

@pytest.fixture
def db_manager(test_db):
    """Provide a real database manager with test database"""
    db = DBManager(db_path=test_db)
    db.connect(connector="sqlite3")
    return db
    # yield db
    # db.disconnect()

@pytest.fixture
def mock_db_manager():
    """Create mock database manager"""
    return Mock()

