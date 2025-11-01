from unittest.mock import Mock, patch
import pytest
import pandas as pd
from src.performance_report import PerformanceCalculator


@pytest.fixture
def performance_calculator(mock_db_manager):
    return PerformanceCalculator(db_manager=mock_db_manager)


@pytest.fixture
def mock_db_data():
    df = pd.DataFrame({
        'fund_name': ['FundA', 'FundB', 'FundA', 'FundB'],
        'eom_date': ['2023-01-31', '2023-01-31', '2023-02-28', '2023-02-28'],
        'fund_mv_end': [1000.0, 2000.0, 1100.0, 1900.0],
        'realized_p_l': [50.0, 20.0, 30.0, -10.0],
    })
    df['eom_date_dt'] = pd.to_datetime(df['eom_date'])
    df = df.sort_values(['fund_name', 'eom_date_dt']).reset_index(drop=True)
    return df

def test_best_performer_identification_structure(performance_calculator, mock_db_data):
    """Ensure output rows match months that have calculable RoR and no nulls in key fields."""
    with patch('src.performance_report.pd.read_sql', return_value=mock_db_data):
        result_df = performance_calculator.run_attribution()

        # compute expected number of months with calculable RoR from mock data
        tmp = mock_db_data.copy()
        tmp = tmp.sort_values(['fund_name', 'eom_date_dt']).reset_index(drop=True)
        tmp['fund_mv_start'] = tmp.groupby('fund_name')['fund_mv_end'].shift(1)
        tmp['rate_of_return'] = (tmp['fund_mv_end'] - tmp['fund_mv_start'] + tmp['realized_p_l']) / tmp['fund_mv_start']
        expected_months = tmp.dropna(subset=['rate_of_return'])['eom_date'].nunique()

        assert len(result_df) == expected_months
        assert 'best_performing_fund_name' in result_df.columns
        assert 'highest_rate_of_return' in result_df.columns
        assert not result_df['best_performing_fund_name'].isnull().any()
        assert not result_df['highest_rate_of_return'].isnull().any()

def test_rate_of_return_calculation(performance_calculator, mock_db_data):
    """Validate RoR computation and best-performer selection for the second period."""
    with patch('src.performance_report.pd.read_sql', return_value=mock_db_data):
        result_df = performance_calculator.run_attribution()

        for col in ('eom_date', 'best_performing_fund_name', 'highest_rate_of_return'):
            assert col in result_df.columns

        # For 2023-02-28 best should be FundA based on fixture:
        feb_row = result_df[result_df['eom_date'] == '2023-02-28'].iloc[0]
        assert feb_row['best_performing_fund_name'] == 'FundA'
        assert round(feb_row['highest_rate_of_return'], 3) == round(((1100.0 - 1000.0 + 30.0) / 1000.0), 3)