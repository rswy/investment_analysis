import os
from src.db_manager import DBManager
from src.data_validation import DataValidator
from src.data_ingestion import DataIngestor
from src.price_reconciliation import PriceReconciler
from src.performance_report import PerformanceCalculator

# Import Logger
from config import LOGGER 
# Import Database Name
from config import DB_NAME
# Import Table Names
from config import FUND_POSITIONS
# Import SQL Files
from config import MASTER_SQL_FILE, FUND_POSITION_FILE
# Import External Funds Data Filepath
from config import EXTERNAL_FUNDS_DATA_DIR
# Import OUTPUT Directory & Files
from config import OUTPUT_DIR, PRICE_RECON_OUTPUT, BEST_PERFORMING_OUTPUT


def run_pipeline():
    
    
    LOGGER.info("--BEGIN INVESTMENT ANALYSIS PIPELINE--")

    # try: 
         
    # 1. Setup Database Connection
    LOGGER.info("\n\n\nStep 1: Setup Database Connection\n\n")
    db = DBManager(db_path=DB_NAME)
    db.connect(connector="sqlite3")

    # 2. Ingest Reference Data
    LOGGER.info("\n\n\nStep 2: Ingest Master Reference Data.\n\nc")
    ingestor = DataIngestor(db_manager=db)
    ingestor.ingest_master_data(MASTER_SQL_FILE)

    # 3. Preprocess Fund Data
    LOGGER.info("\n\n\nStep 3. Preprocess Fund Data.\n\n")
    validator = DataValidator(db_manager=db)
    preprocessed_fund_df = validator.batch_preprocessing_csv(EXTERNAL_FUNDS_DATA_DIR,FUND_POSITION_FILE)
    LOGGER.info(f"Total preprocessed fund data shape: {preprocessed_fund_df.shape}")

    # 4. Ingest Fund Data
    LOGGER.info("\n\n\nStep 4: Ingest processed Fund Data.\n\n")
    ingestor = DataIngestor(db_manager=db)
    ingestor.ingest_dataframe(preprocessed_fund_df,FUND_POSITIONS) 


    # 5. Price Reconciliation
    LOGGER.info("\n\n\nStep 5: Perform Price Reconciliation.\n\n")
    reconciler = PriceReconciler(db_manager=db)
    final_reconciliation_df = reconciler.run_reconciliation()
    final_reconciliation_df.to_csv(PRICE_RECON_OUTPUT)
    

    # 6. Performance Attribution (Rate of Return Calculation)
    LOGGER.info("\n\n\nStep 6: Perform Performance Attribution (Rate of Return Calculation).\n\n")
    calculator = PerformanceCalculator(db_manager=db)
    best_performer_df = calculator.run_attribution()
    if not best_performer_df.empty:
        calculator.save_output(best_performer_df, BEST_PERFORMING_OUTPUT)
    else:
        LOGGER.warning("No best performer data to save.")

    # 7. Check if connection is still open and close database connection
    db.disconnect()
    LOGGER.info("Database connection closed after master data ingestion.")



    # except Exception as e:
    #     LOGGER.critical(f"Pipeline failed due to a critical error: {e}")



    LOGGER.info("--- Pipeline Completed Successfully ---")

if __name__=="__main__":
    run_pipeline()






