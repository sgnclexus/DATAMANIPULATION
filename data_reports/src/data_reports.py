import cx_Oracle
import pandas as pd
from utils.logger import Logger
import db_connection import DB_Conn
import time

if __name__ == '__main__':
    start_time = time.time()
    logger = Logger()
    sp = DB_Conn()
    logger.log_event(message="Esto es una prueba")
    
    