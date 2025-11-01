# performance_calculator.py
import pandas as pd
from src.db_manager import DBManager
from config import LOGGER, FUND_POSITIONS

class PerformanceCalculator:
    """Calculates monthly Rate of Return (RoR) for all funds."""
    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager
        LOGGER.info("PerformanceCalculator initialized.")

    def run_attribution(self):
        """Calculates RoR for all funds and identifies the best performer each month."""
        
        # 1. Aggregate Fund Market Value (MV) and Total Realized P/L
        aggregation_query = f"""
        SELECT
            eom_date,
            fund_name,
            SUM(market_value) AS fund_mv_end,
            SUM(realised_p_l) AS realized_p_l
        FROM {FUND_POSITIONS}
        GROUP BY 1, 2
        ORDER BY eom_date, fund_name;
        """
        
        fund_performance_df = pd.read_sql(aggregation_query, self.db_manager.conn)
        
        if fund_performance_df.empty:
            LOGGER.warning("No fund performance data found for attribution.")
            return pd.DataFrame()

        # 2. Calculate Fund MV Start (Fund_MV_end from M-1)
        
        # Ensure correct date ordering for the lag calculation
        fund_performance_df['eom_date_dt'] = pd.to_datetime(fund_performance_df['eom_date'])
        fund_performance_df = fund_performance_df.sort_values(by=['fund_name', 'eom_date_dt'])

        # Create the lagged column (MV_end of previous month)
        fund_performance_df['fund_mv_start'] = fund_performance_df.groupby('fund_name')['fund_mv_end'].shift(1)
        
        # The very first month for each fund will have a NaN start MV and thus RoR cannot be calculated.
        # This is expected for the first report date (Dec 2022).
        
        # 3. Calculate Rate of Return (RoR)
        # RoR = (Fund_MV_end - Fund_MV_start + Realized P/L) / Fund_MV_start
        fund_performance_df['rate_of_return'] = (
            fund_performance_df['fund_mv_end'] - fund_performance_df['fund_mv_start'] + fund_performance_df['realized_p_l']
        ) / fund_performance_df['fund_mv_start']
        
        # Drop rows where RoR couldn't be calculated (first month of each fund)
        ror_df = fund_performance_df.dropna(subset=['rate_of_return']).copy()
        
        # 4. Identify the Best Performing Fund for each Month
        
        # Group by month and find the index of the max RoR within each group
        best_performer_indices = ror_df.groupby('eom_date_dt')['rate_of_return'].idxmax()
        best_performers = ror_df.loc[best_performer_indices]
        
        # Select and rename final columns
        output_df = best_performers[[
            'eom_date', 
            'fund_name', 
            'rate_of_return'
        ]].rename(columns={
            'fund_name': 'best_performing_fund_name',
            'rate_of_return': 'highest_rate_of_return'
        })
        
        LOGGER.info("Performance attribution completed.")
        return output_df

    def save_output(self, df, filename):
        """Saves the best-performing funds DataFrame to a CSV file."""
        df.to_csv(filename, index=False)
        LOGGER.info(f"Best performing funds report saved to {filename}")