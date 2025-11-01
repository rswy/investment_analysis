import pytest
from src.data_ingestion import DataIngestor
import os
import pandas as pd

@pytest.fixture
def sample_fund_data():
    """Provide sample fund data for testing"""
    return pd.DataFrame({
        'fund_name': ['A', 'B'],
        'eom_date': ['2023-01-31', '2023-01-31'],
        'market_value': [1000.0, 2000.0],
        'price': [100.0, 200.0],
        'symbol': ['AAPL', 'GOOGL'],
        'quantity': [10, 10],
        'realised_p_l': [50.0, -20.0]
    })


def test_ingest_fund_data(db_manager, sample_fund_data):
    """Test fund data ingestion using fixtures"""
    ingestor = DataIngestor(db_manager)
    result = ingestor.ingest_dataframe(sample_fund_data, "sample_funds")
    assert result == True

def test_master_data_ingestion(db_manager):
    """Test master data ingestion"""
    ingestor = DataIngestor(db_manager)
    sql_fp= os.path.join('sql','master-reference-sql.sql')
    assert ingestor.ingest_master_data(sql_fp) == True

