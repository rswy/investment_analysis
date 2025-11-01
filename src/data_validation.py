

import pandas as pd
from src.db_manager import DBManager
from config import LOGGER

import re
import os

class DataValidator:
    """Validates and preprocesses fund data before loading into the database."""
    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager
        LOGGER.info("DataValidator initialized.")

    def _preprocess_dataframe(self, df, fund_name, eom_date):
        """Standardizes columns and performs basic data quality checks."""
        
        # Standardize column names (make lowercase and replace non-alphanumeric with '_')
        df.columns = df.columns.str.lower().str.replace(r'[^a-z0-9]+', '_', regex=True).str.strip('_')

        # Drop columns that are entirely null or not needed for core analysis
        df = df.dropna(axis=1, how='all')

        # Add identifying columns
        df['fund_name'] = fund_name
        df['eom_date'] = eom_date
        
        # Clean symbol/security name prefixes (e.g., 'X_', 'SEC-', 'FIN-')
        for col in ['security_name', 'symbol']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(r'(X_|SEC-|FIN-)', '', regex=True).str.strip()

        # Data Quality Check: Missing Market Value
        missing_mv = df['market_value'].isnull().sum()
        if missing_mv > 0:
            LOGGER.warning(f"DQ Alert: Fund {fund_name} on {eom_date} has {missing_mv} rows with missing 'market_value'. These rows will be dropped.")
            df = df.dropna(subset=['market_value', 'realised_p_l'])
        
        # Ensure correct data types before loading
        numeric_cols = ['price', 'quantity', 'realised_p_l', 'market_value']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Fill NaN for Realised P/L and Quantity with 0.0 for safety in aggregation
        df['realised_p_l'] = df['realised_p_l'].fillna(0.0)
        df['quantity'] = df['quantity'].fillna(0.0)

        # Select only required columns for the fund_positions table
        required_cols = [
            'fund_name', 'eom_date', 'financial_type', 'symbol', 'security_name', 
            'sedol', 'isin', 'price', 'quantity', 'realised_p_l', 'market_value'
        ]
        return df[[col for col in required_cols if col in df.columns]]

    def _extract_fund_info(self, filename):
        """
        Extracts fund name and EOM date from filename, normalizing to ISO format YYYY-MM-DD.
        
        Handles formats:
        - Standard: Fund Name.YYYY-MM-DD.csv
        - Report style: rpt-FundName.YYYY-MM-DD.csv
        - Details style: Fund Name.MM-DD-YYYY - details.csv
        - Monthly style: TT_monthly_FundName.YYYYMMDD.csv
        """
        import re
        from datetime import datetime

        def normalize_date(parts):
            """
            Convert date parts to ISO format YYYY-MM-DD, handling different orderings
            parts: dict with 'year', 'month', 'day' keys
            """
            year, month, day = int(parts['year']), int(parts['month']), int(parts['day'])
            
            # If year is clearly identified (4 digits), check if month/day need swapping
            if 1900 <= year <= 2100:
                # If month value > 12, it must be the day
                if month > 12:
                    month, day = day, month
            else:
                # If we have a 2-digit year, assume the largest number is the year
                candidates = [year, month, day]
                if max(candidates) > 31:  # Must be year
                    year = max(candidates)
                    others = [x for x in candidates if x != year]
                    month, day = others if others[0] <= 12 else others[::-1]

            # Validate final date components
            if not (1900 <= year <= 2100):
                raise ValueError(f"Year {year} outside valid range (1900-2100)")
            if not (1 <= month <= 12):
                raise ValueError(f"Month {month} outside valid range (1-12)")
            if not (1 <= day <= 31):
                raise ValueError(f"Day {day} outside valid range (1-31)")
                
            return f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}"

        try:
            # Strip path and extension
            base = os.path.splitext(os.path.basename(filename))[0]
            
            # Common date patterns
            date_patterns = [
                # YYYY-MM-DD or YYYY.MM.DD
                r'(?P<year>\d{4})[-.](?P<month>\d{1,2})[-.](?P<day>\d{1,2})',
                # MM-DD-YYYY or DD-MM-YYYY (ambiguous, resolve in normalize_date)
                r'(?P<first>\d{1,2})[-.](?P<second>\d{1,2})[-.](?P<year>\d{4})',
                # YYYYMMDD
                r'(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})',
                # MM_DD_YYYY or DD_MM_YYYY
                r'(?P<first>\d{1,2})_(?P<second>\d{1,2})_(?P<year>\d{4})',
            ]

            # Extract date using patterns
            date_match = None
            date_parts = None
            
            for pattern in date_patterns:
                match = re.search(pattern, base)
                if match:
                    date_match = match
                    parts = match.groupdict()
                    if 'first' in parts:
                        # For ambiguous patterns, try month-first then day-first
                        try:
                            parts = {'year': parts['year'], 'month': parts['first'], 'day': parts['second']}
                            iso_date = normalize_date(parts)
                            date_parts = parts
                            break
                        except ValueError:
                            parts = {'year': parts['year'], 'month': parts['second'], 'day': parts['first']}
                            iso_date = normalize_date(parts)
                            date_parts = parts
                            break
                    else:
                        iso_date = normalize_date(parts)
                        date_parts = parts
                        break

            if not date_match:
                raise ValueError(f"No valid date pattern found in filename: {filename}")

            # Extract fund name: remove date part and clean up
            fund_name = base.split(date_match.group())[0]
            
            # Clean up fund name
            fund_name = (fund_name
                .replace('Fund ', '')
                .replace('Report-of-', '')
                .replace('mend-report ', '')
                .replace('rpt-', '')
                .replace('TT_monthly_', '')
                .strip(' .-_')
            )

            LOGGER.debug(f"Extracted: fund='{fund_name}', date='{iso_date}' from '{filename}'")
            return fund_name, iso_date

        except Exception as e:
            LOGGER.error(f"Failed to extract info from filename '{filename}': {e}")
            raise
  

    def batch_preprocessing_csv(self, fund_data_filepath, fund_table_script_path):
        """Ingests all fund report CSV files."""

        final_df = pd.DataFrame()

        # Create fund_positions table if not exists
        self.db_manager.execute_script(fund_table_script_path)
        
        for file_name in os.listdir(fund_data_filepath):

            if not file_name.lower().endswith('.csv'):
                LOGGER.info(f"Skipping non-CSV file: {file_name}")
                continue

            fp = os.path.join(fund_data_filepath,file_name)

            try:
                fund_name, eom_date = self._extract_fund_info(file_name)

                # LOGGER.info(f"Processing file: {file_name} (Fund: {fund_name}, Date: {eom_date})")
                
                df = pd.read_csv(fp)
                preprocessed_df = self._preprocess_dataframe(df, fund_name, eom_date)
                final_df = pd.concat([final_df, preprocessed_df], ignore_index=True)
                            
            except Exception as e:
                LOGGER.error(f"Fatal error processing file {file_name}: {e}")
                # Continue to next file
        
        return final_df
    
