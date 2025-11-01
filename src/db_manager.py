import os
import sqlite3
from config import DB_NAME, LOGGER, FUND_POSITIONS

class DBManager:

    def __init__(self, db_path = None, sql_script_path = None):
        self.conn = None
        self.db_path = db_path
    
    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            print("Connection Closed.")

    def connect(self,connector="sqlite3"):
        if connector == "sqlite3":
            try:
                self.conn = sqlite3.connect(self.db_path)
                if self.conn:
                    print(f"Connected to database: {self.db_path}")
            except sqlite3.Error as e:
                LOGGER.error(f"Database connection error: {e}")
                raise
            # finally:
            #     if self.conn:
            #         self.disconnect()
    
    def execute_script(self, sql_script_path=None):
        try:
            with open(sql_script_path, 'r') as f:
                sql_script = f.read()
                self.conn.executescript(sql_script)
            self.conn.commit()
        except sqlite3.Error as e:
            LOGGER.error(f"SQL Script Execution failed for {sql_script_path}")
            raise
        LOGGER.info(f"Successfully executed SQL script: {sql_script_path}")
        # print(f"Successfully executed SQL script: {sql_script_path}")

    
    def execute_sql_string(self,query):
        if self.conn:
            cursor = self.conn.cursor()
            try:
                cursor.execute(query)
                if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                    self.conn.commit()
                    return cursor.rowcount
                rows =  cursor.fetchall()
                if rows:
                    columns = [description[0] for description in cursor.description]
                    result = [dict(zip(columns, row)) for row in rows]
                    return result

            except sqlite3.Error as e:
                LOGGER.error(f"SQL String Query Execution failed for {query}")
                raise
        else:
            print("No connection established!")
            return None
        

