import cx_Oracle
from configparser import ConfigParser
from utils.logger import Logger


class DB_Conn:

    # Para mas informacion: https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html
    # oracle_ic_path = os.getenv("ORACLE_IC")
    oracle_ic_path = ""
    cx_Oracle.init_oracle_client(lib_dir=oracle_ic_path)    
        

    def get_db_info(filename,section):
        # instantiating the parser object
            
        parser=ConfigParser()
        parser.read(filename)
        db_info={}
        if parser.has_section(section):
            # items() method returns (key,value) tuples
            key_val_tuple = parser.items(section) 
            for item in key_val_tuple:
                db_info[item[0]]=item[1] # index 0: key & index 1: value

        
        return db_info    

    def connect(db_connection_info):

        """ Connect to Oracle database server """
        try:
            # connect to the Oracle server
            print('Connecting to the Oracle database...')
            section='oracle'
            db_info = db_connection_info.get_db_info(db_connection_info,section)
            
            dsn = cx_Oracle.makedsn(db_info["servidor"], db_info["puerto"], sid=db_info["sid"])
            connection = cx_Oracle.connect(
                db_info["usuario"],
                db_info["password"],
                dsn=dsn,
                encoding=db_info["encoding"])
            
            # create a cursor
            cur = connection.cursor()
            return cur, connection
        except (Exception, cx_Oracle.Error) as error:
            Logger.log_error(error)
            print(error)
            return None
        

    def get_all_by_query(cursor, query):
        try:
            cursor.execute(query)
            col_names = [row[0] for row in cursor.description]
            values = cursor.fetchall()
            return pd.DataFrame(values, columns=col_names)
        except cx_Oracle.DatabaseError as err:
            msg = f"Ocurrió un error al ejecutar el siguiente query: {query}"
            Logger.log_error(msg)
            print(msg)            
            msg = f"Error:  {err}"
            print(msg)
            Logger.log_error(msg)
            return None


    def execute_st(self,cur, conection, year, aplication_number):
        n = 1
        try:
            cur.callproc("APPDCGRL.PKG_CALIFICA_MST_DEV.SP_GENERAL_APLICACION", [year, aplication_number, None, None])
            n = n+1
            print("Store 1", [year, aplication_number, None, None])
            conection.commit()
            return True
        except cx_Oracle.Error as error:
            msg = f"Ocurrio un error en el paso {n}"
            print(msg)            
            print(error)            
            Logger.log_error(msg)
            Logger.log_error(error)
            
            return False