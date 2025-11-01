import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch
from src.price_reconciliation import PriceReconciler



@pytest.fixture
def fund_instruments_df():
    """Sample fund positions data"""
    return pd.DataFrame({
        'eom_date': ['2023-01-31', '2023-01-31', '2023-01-31'],
        'financial_type': ['EQUITY', 'BOND', 'EQUITY'],
        'identifier': ['AAPL', 'BOND1', 'GOOGL'],
        'fund_name': ['Fund A', 'Fund A', 'Fund B'],
        'reported_price': [150.0, 95.0, 2100.0]
    })

@pytest.fixture
def master_prices_df():
    """Sample master reference prices data"""
    return pd.DataFrame({
        'eom_date': ['2023-01-31', '2023-01-31', '2023-01-15'],
        'identifier': ['AAPL', 'BOND1', 'GOOGL'],
        'master_price': [150.0, 96.0, 2000.0],
        'financial_type_ref': ['EQUITY', 'BOND', 'EQUITY']
    })



def test_price_reconciliation_basic(mock_db_manager, fund_instruments_df, master_prices_df):
    """
    Test basic price reconciliation functionality:
    - Perfect match (AAPL)
    - Price difference (BOND1)
    - LAP case (GOOGL)
    """
    reconciler = PriceReconciler(mock_db_manager)
    
    # Mock the SQL query results
    with patch('pandas.read_sql') as mock_read_sql:
        mock_read_sql.side_effect = [fund_instruments_df, master_prices_df]
        
        # Run reconciliation
        result_df = reconciler.run_reconciliation()
        
    # Verify output structure
    expected_columns = [
        'fund_name', 'eom_date', 'financial_type', 'identifier',
        'reported_price', 'master_price_filled', 'price_difference'
    ]
    assert all(col in result_df.columns for col in expected_columns)
    
    # Test case 1: Perfect match (AAPL)
    aapl_row = result_df[result_df['identifier'] == 'AAPL'].iloc[0]
    assert aapl_row['price_difference'] == 0.0
    
    # Test case 2: Price difference (BOND1)
    bond_row = result_df[result_df['identifier'] == 'BOND1'].iloc[0]
    assert bond_row['price_difference'] == -1.0  # 95.0 - 96.0
    
    # Test case 3: Last Available Price (GOOGL)
    googl_row = result_df[result_df['identifier'] == 'GOOGL'].iloc[0]
    assert googl_row['master_price_filled'] == 2000.0  # Should use LAP from 01-15

def test_price_reconciliation_edge_cases(mock_db_manager):
    """
    Test edge cases:
    - Empty input data
    - Missing master prices
    - Invalid price formats
    """
    reconciler = PriceReconciler(mock_db_manager)
    
    # Test case 1: Empty fund positions
    empty_positions = pd.DataFrame(columns=['eom_date', 'financial_type', 'identifier', 
                                          'fund_name', 'reported_price'])
    empty_master = pd.DataFrame(columns=['eom_date', 'identifier', 'master_price', 
                                       'financial_type_ref'])
    
    with patch('pandas.read_sql') as mock_read_sql:
        mock_read_sql.side_effect = [empty_positions, empty_master]
        result_df = reconciler.run_reconciliation()
        
    assert len(result_df) == 0
    assert isinstance(result_df, pd.DataFrame)
    
    # Test case 2: Missing master prices
    positions_df = pd.DataFrame({
        'eom_date': ['2023-01-31'],
        'financial_type': ['EQUITY'],
        'identifier': ['MISSING'],
        'fund_name': ['Fund A'],
        'reported_price': [100.0]
    })
    
    master_df = pd.DataFrame({
        'eom_date': [],
        'identifier': [],
        'master_price': [],
        'financial_type_ref': []
    })
    
    with patch('pandas.read_sql') as mock_read_sql:
        mock_read_sql.side_effect = [positions_df, master_df]
        result_df = reconciler.run_reconciliation()
        
    assert len(result_df) == 1
    assert pd.isna(result_df.iloc[0]['master_price_filled'])
    assert pd.isna(result_df.iloc[0]['price_difference'])











# # Added edge-case tests: invalid price formats and master_price string forward-fill

# def test_invalid_price_formats_and_coercion(mock_db_manager):
#     """
#     Positions and master prices contain invalid/non-numeric strings.
#     After coercion to numeric:
#     - Valid numeric pairs produce numeric price_difference
#     - Any side with non-numeric coerces to NaN and results in NaN price_difference
#     """
#     reconciler = PriceReconciler(mock_db_manager)

#     positions = pd.DataFrame({
#         'eom_date': ['2023-01-31', '2023-01-31', '2023-01-31', '2023-01-31'],
#         'financial_type': ['EQUITY'] * 4,
#         'identifier': ['S1', 'S2', 'S3', 'S4'],
#         'fund_name': ['F1', 'F1', 'F1', 'F1'],
#         'reported_price': ['100.0', 'N/A', None, '200']  # S1 numeric-string, S2 invalid, S3 None, S4 numeric-string
#     })

#     master = pd.DataFrame({
#         'eom_date': ['2023-01-31', '2023-01-31', '2023-01-31'],
#         'identifier': ['S1', 'S2', 'S4'],
#         'master_price': ['100', 'abc', '199.5'],
#         'financial_type_ref': ['EQUITY'] * 3
#     })

#     with patch('pandas.read_sql', side_effect=[positions, master]):
#         out = reconciler.run_reconciliation()

#     # S1: both numeric -> diff 0
#     s1 = out[out['identifier'] == 'S1'].iloc[0]
#     assert s1['price_difference'] == pytest.approx(0.0)

#     # S2: reported 'N/A' coerces to NaN, master 'abc' coerces to NaN -> price_difference NaN
#     s2 = out[out['identifier'] == 'S2'].iloc[0]
#     assert pd.isna(s2['master_price_filled'])
#     assert pd.isna(s2['price_difference'])

#     # S3: master missing -> master_price_filled NaN and price_difference NaN
#     s3 = out[out['identifier'] == 'S3'].iloc[0]
#     assert pd.isna(s3['master_price_filled'])
#     assert pd.isna(s3['price_difference'])

#     # S4: numeric strings on both sides -> numeric diff (200 - 199.5 = 0.5)
#     s4 = out[out['identifier'] == 'S4'].iloc[0]
#     assert s4['price_difference'] == pytest.approx(0.5)

# def test_master_price_string_forward_fill(mock_db_manager):
#     """
#     Ensure master prices provided as strings are coerced and forward-filled correctly.
#     Master has an earlier price as a string; fund report date is later and should pick last available price.
#     """
#     reconciler = PriceReconciler(mock_db_manager)

#     # Master price available on 2023-01-15 as string, nothing on 01-31 for that identifier
#     master = pd.DataFrame({
#         'eom_date': ['2023-01-15'],
#         'identifier': ['X1'],
#         'master_price': ['123.45'],
#         'financial_type_ref': ['EQUITY']
#     })

#     positions = pd.DataFrame({
#         'eom_date': ['2023-01-31'],
#         'financial_type': ['EQUITY'],
#         'identifier': ['X1'],
#         'fund_name': ['FundX'],
#         'reported_price': [123.45]
#     })

#     with patch('pandas.read_sql', side_effect=[positions, master]):
#         out = reconciler.run_reconciliation()

#     row = out[out['identifier'] == 'X1'].iloc[0]
#     # master_price_filled should be numeric 123.45 after coercion and forward-fill
#     assert row['master_price_filled'] == pytest.approx(123.45)
#     # price_difference should be ~0
#     assert row['price_difference'] == pytest.approx(0.0)