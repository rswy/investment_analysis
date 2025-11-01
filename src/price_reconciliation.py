# price_reconciler.py
import pandas as pd
from src.db_manager import DBManager
from config import LOGGER, FUND_POSITIONS, EQUITY_PRICES, BOND_PRICES
import os




class PriceReconciler:
    """Calculates the difference between reported prices and master reference prices."""
    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager
        LOGGER.info("PriceReconciler initialized.")

    def run_reconciliation(self):
        """Executes the price reconciliation logic against the master reference data."""
        
        # Step 1. Query Data And Standardize the Identifier and Dates

        # Query fund_positions table for relevant fields
        instruments_query = f"""
        SELECT DISTINCT
            t1.eom_date,
            t1.financial_type,
            COALESCE(t1.symbol, t1.isin) as identifier,
            t1.fund_name,
            t1.price AS reported_price
        FROM {FUND_POSITIONS} t1;
        """
        # Query Equity Prices and Bond Prices for available master prices
        master_prices_query = f"""
        SELECT
            DATETIME AS eom_date,
            SYMBOL AS identifier,
            PRICE AS master_price,
            'EQUITY' as financial_type_ref
        FROM {EQUITY_PRICES}
        UNION ALL
        SELECT
            DATETIME AS eom_date,
            ISIN AS identifier,
            PRICE AS master_price,
            'BOND' as financial_type_ref
        FROM {BOND_PRICES};
        """

        # Query and initial data preparation
        fund_instruments = pd.read_sql(instruments_query, self.db_manager.conn)
        master_prices_df = pd.read_sql(master_prices_query, self.db_manager.conn)
        
        # Convert prices to float64 for accurate comparison
        for df in [fund_instruments, master_prices_df]:
            for col in ['reported_price', 'master_price']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Standardize dates and identifiers 
        for df in [fund_instruments, master_prices_df]:
            df['eom_date'] = pd.to_datetime(df['eom_date'], format='mixed')
            df['identifier'] = df['identifier'].fillna('').astype(str).str.strip()

            
    # 2. Prepare for Last Available Price (LAP) logic
        # Create a temporary DF with fund positions' dates, but no master price
        fund_lap_points = fund_instruments[['identifier', 'eom_date']].copy()
        fund_lap_points['is_fund_report'] = True
        
        # Prepare master prices for LAP
        master_prices_df['is_fund_report'] = False
        
        # Combine all time points and sort chronologically
        lap_df = pd.concat([
            master_prices_df[['identifier', 'eom_date', 'master_price', 'is_fund_report']],
            fund_lap_points
        ]).sort_values(['identifier', 'eom_date'])
        
        # Apply forward fill within each identifier group
        lap_df['master_price_filled'] = lap_df.groupby('identifier')['master_price'].ffill()
        
        # Extract only the fund report dates with their filled prices
        reconciliation_df = lap_df[lap_df['is_fund_report']].copy()
        
        # 3. Final merge and price comparison
        final_df = fund_instruments.merge(
            reconciliation_df[['identifier', 'eom_date', 'master_price_filled']],
            on=['identifier', 'eom_date'],
            how='left'
        )

        print(f"!!!!!!!LENGTH OF FINAL DF: {len(final_df)}")
        
        # Calculate price differences
        final_df['price_difference'] = (
            final_df['reported_price'].astype(float) - 
            final_df['master_price_filled'].astype(float)
        )
        
        # Debug logging
        no_master_price = final_df['master_price_filled'].isna().sum()
        if no_master_price:
            LOGGER.warning(f"{no_master_price} positions have no master price (either exact or last available)")
            LOGGER.debug("Sample of unmatched positions:")
            LOGGER.debug(final_df[final_df['master_price_filled'].isna()][
                ['identifier', 'eom_date', 'reported_price']
            ].head())
        
        # Calculate statistics
        diff_stats = {
            'total_positions': len(final_df),
            'positions_with_diffs': (final_df['price_difference'].abs() > 0.0001).sum(),
            'max_diff': final_df['price_difference'].abs().max(),
            'mean_diff': final_df['price_difference'].abs().mean()
        }
        LOGGER.info(f"\nPrice difference statistics:\n{pd.Series(diff_stats)}")
        
        # Prepare output
        output_cols = [
            'fund_name', 'eom_date', 'financial_type', 'identifier',
            'reported_price', 'master_price_filled', 'price_difference'
        ]
        
        result_df = final_df[output_cols].copy()
        
        # Log results
        LOGGER.info(f"Total positions processed: {len(result_df)}")
        LOGGER.info(f"Positions with price differences: {(result_df['price_difference'].abs() > 0.0001).sum()}")
        
        return result_df

 