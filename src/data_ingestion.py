# data_ingestor.py
import pandas as pd
import re
import os
from src.db_manager import DBManager
from src.data_validation import DataValidator
from config import LOGGER, FUND_POSITIONS

class DataIngestor:
   
    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager
        LOGGER.info("DataIngestor initialized.")

    def ingest_master_data(self, sql_script_path):
        self.db_manager.execute_script(sql_script_path)
        LOGGER.info("Master Reference Data Ingested.")
        return True
                                       

    def ingest_dataframe(self, df, table_name):
        try:
            conn = self.db_manager.conn
            df.to_sql(table_name, conn, if_exists='append', index=False)
            LOGGER.info(f"Successfully inserted {len(df)} rows into '{table_name}'.")
            return True
        except Exception as e:
            LOGGER.error(f"Failed to insert data into '{table_name}': {e}")
            raise
