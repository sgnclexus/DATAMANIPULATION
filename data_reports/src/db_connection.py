import os
import cx_Oracle
import pandas as pd
import mysql.connector
from mysql.connector import errorcode   
from configparser import ConfigParser
from utils.logger import Logger
from sqlalchemy import create_engine
# from dotenv import load_dotenv

class DB_Conn:

    # def __init__(self, db_connection=os.getenv('DATABASE_CONNECTION'), db_type="", host="", port="", database="", user="", password=""):
    def __init__(self, db_connection=os.getenv('DATABASE_CONNECTION'), db_type=""):  

        # Para mas informacion: https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html    
        oracle_ic_path = os.getenv("ORACLE_IC")
        cx_Oracle.init_oracle_client(lib_dir=oracle_ic_path)    

        # cwd = os.getcwd()
        # Construct the absolute path
        # file_path = os.path.join(cwd, 'database.ini')

        self.db_type = db_type
        self.Data_Connection = DB_Conn.get_db_info(db_connection,db_type)       
        print(self.Data_Connection)         
        self.host = self.Data_Connection["servidor"]
        self.port = self.Data_Connection["puerto"]
        self.sid = self.Data_Connection["sid"]
        self.user = self.Data_Connection["usuario"]
        self.password = self.Data_Connection["password"]
        self.encoding = self.Data_Connection["encoding"]
        # self.host = "califica.db.ceneval.edu.mx"
        # self.port = 1530
        # self.database = "papps001"
        # self.user = "APPCALIFICA"
        # self.password = "8Nj+WLGQ"

        
        

    def get_db_info(filename,section):

        print('Entre a get_db_info con estos parametros : ', filename, " - ", section)                    
        parser=ConfigParser()
        parser.read(filename)
        db_info={}

        if parser.has_section(section):
            print('Entre a if ')                    
            # items() method returns (key,value) tuples
            key_val_tuple = parser.items(section) 
            for item in key_val_tuple:
                db_info[item[0]]=item[1] # index 0: key & index 1: value

        
        return db_info    

    # def connect(db_connection_info):

    #     """ Connect to Oracle database server """
    #     try:
    #         # connect to the Oracle server
    #         print('Connecting to the Oracle database...')
    #         section='oracle'
    #         db_info = db_connection_info.get_db_info(db_connection_info,section)
            
    #         dsn = cx_Oracle.makedsn(db_info["servidor"], db_info["puerto"], sid=db_info["sid"])
    #         connection = cx_Oracle.connect(
    #             db_info["usuario"],
    #             db_info["password"],
    #             dsn=dsn,
    #             encoding=db_info["encoding"])
            
    #         # create a cursor
    #         cur = connection.cursor()
    #         return cur, connection
    #     except (Exception, cx_Oracle.Error) as error:
    #         Logger.log_error(error)
    #         print(error)
    #         return None
        
    def connect(self):
        if self.db_type == 'oracle':
        
            # db_info = self.get_db_info(self.db_connection,self.db_type)
            dsn_tns = cx_Oracle.makedsn(self.host, self.port, service_name=self.sid)
            self.connection = cx_Oracle.connect(user=self.user, password=self.password, dsn=dsn_tns, encoding=self.encoding)
        
        elif self.db_type == 'mysql':

            try:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Something is wrong with your user name or password")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    print("Database does not exist")
                else:
                    print(err)        

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def select_query(self, query):

        if not self.connection:
            self.connect()
        
        if self.db_type == 'oracle':
            
            cursor = self.connection.cursor()
            cursor.execute(query)
            col_names = [row[0] for row in cursor.description]
            values = cursor.fetchall()
            return pd.DataFrame(values, columns=col_names)
        
            # df = pd.read_sql(query, con=self.connection)      # Another way to connect but we need to call the engine

        elif self.db_type == 'mysql':
            
            cursor = self.connection.cursor()
            cursor.execute(query)
            columns = cursor.column_names
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)
            cursor.close()

            return df
        
        self.disconnect()


    # def get_all_by_query(cursor, query):
    #     try:
    #         cursor.execute(query)
    #         col_names = [row[0] for row in cursor.description]
    #         values = cursor.fetchall()
    #         return pd.DataFrame(values, columns=col_names)
    #     except cx_Oracle.DatabaseError as err:
    #         msg = f"Ocurrió un error al ejecutar el siguiente query: {query}"
    #         Logger.log_error(msg)
    #         print(msg)            
    #         msg = f"Error:  {err}"
    #         print(msg)
    #         Logger.log_error(msg)
    #         return None


    def execute_stored_procedure(self, procedure_name, params):

        if not self.connection:
            self.connect()

        if self.db_type == 'oracle':
            
            cursor = self.connection.cursor()            
            output = cursor.var(cx_Oracle.CURSOR)
            # refCursor = self.connection.cursor()
            #params.append(output)
            # params = [2019,"074304312",output,10001]
            cursor.callproc(procedure_name, params)
            result = output.getvalue()            
            df = pd.DataFrame(data=result.fetchall(), columns=[desc[0] for desc in result.description])
            
            cursor.close()

        elif self.db_type == 'mysql':
            
            cursor = self.connection.cursor()
            cursor.callproc(procedure_name, params)
            
            for result in cursor.stored_results():
                columns = result.column_names
                rows = result.fetchall()

            df = pd.DataFrame(rows, columns=columns)
            cursor.close()

        return df        
    

    def execute_rg_application(self, procedure_name, params):

        if not self.connection:
            self.connect()

    
            
        cursor = self.connection.cursor()            
        output = cursor.var(cx_Oracle.CURSOR)
        # refCursor = self.connection.cursor()
        #params.append(output)
        params.insert(2,output)
        cursor.callproc(procedure_name, params)
        result = output.getvalue()            
        df = pd.DataFrame(data=result.fetchall(), columns=[desc[0] for desc in result.description])
        
        cursor.close()

    

        return df            