import pytest
from src.data_validation import DataValidator
import pandas as pd



@pytest.fixture
def sample_unprocessed_fund_data():
    """Sample fund data before preprocessing"""
    return pd.DataFrame({
        'SECURITY_NAME': ['APPLE INC', 'MICROSOFT CORP'],
        'MARKET_VALUE': [1000.0, 2000.0],
        'PRICE': [150.0, 200.0],
        'QUANTITY': [10, 15],
        'REALISED_P_L': [50.0, -20.0],
        'SYMBOL': ['X_AAPL', 'SEC-MSFT'],
        'FINANCIAL_TYPE': ['Equities', 'Equities']
    })


def test_extract_fund_info():
    """Test fund name and date extraction from different filename formats"""
    validator = DataValidator(db_manager=None)
    
    test_cases = [
        ('FundA.2023-01-31.csv', ('FundA', '2023-01-31')),
        ('rpt-GlobalFund.2023-12-25.csv', ('GlobalFund', '2023-12-25')),
        ('TT_monthly_AsiaFund.20230228.csv', ('AsiaFund', '2023-02-28'))
    ]
    
    for filename, expected in test_cases:
        fund_name, date = validator._extract_fund_info(filename)
        assert (fund_name, date) == expected, f"Failed for filename: {filename}"


def test_preprocess_dataframe(sample_unprocessed_fund_data):
    """Test data preprocessing functionality"""
    validator = DataValidator(db_manager=None)
    
    # Process the sample data
    processed_df = validator._preprocess_dataframe(
        df=sample_unprocessed_fund_data,
        fund_name='TestFund',
        eom_date='2023-01-31'
    )
    
    # Verify preprocessing results
    assert 'fund_name' in processed_df.columns
    assert 'eom_date' in processed_df.columns
    assert processed_df['fund_name'].iloc[0] == 'TestFund'
    assert processed_df['eom_date'].iloc[0] == '2023-01-31'
    
    # Verify column cleaning
    assert 'X_' not in processed_df['symbol'].iloc[0]
    assert 'SEC-' not in processed_df['symbol'].iloc[1]
    
    # Verify numeric columns
    assert pd.api.types.is_numeric_dtype(processed_df['market_value'])
    assert pd.api.types.is_numeric_dtype(processed_df['price'])