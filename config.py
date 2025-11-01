import logging
import os

# General 
DB_NAME = 'db/investment_strategy.db'
LOG_FILE = 'pipeline.log'
OUTPUT_DIR = 'output'

# SQL
MASTER_SQL_FILE = 'sql/master-reference-sql.sql'
FUND_POSITION_FILE = 'sql/create_fund_position_table.sql'
# DB TABLES 
EQUITY_PRICES = 'equity_prices'
BOND_PRICES = 'bond_prices'
FUND_POSITIONS = 'fund_positions'

# INPUT FILEPATH FOR EXTERNAL FUNDS DATA
EXTERNAL_FUNDS_DATA_DIR = 'external-funds'

# OUTPUT FILE NAMES
PRICE_RECON_OUTPUT = os.path.join(OUTPUT_DIR,"price_reconciliation.csv")
BEST_PERFORMING_OUTPUT = os.path.join(OUTPUT_DIR,"best_performing_funds.csv")


# LOGGING CONFIG
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE,mode ='w'),
        logging.StreamHandler()
    ]
)
LOGGER = logging.getLogger('InvestmentPipeline')
