import pandas as pd
import time
from pathlib import Path
from db_connection import DB_Conn
from utils.loader import Loader
from utils.logger import Logger
from utils.StringSplitter import StringSplitter

GET_DATA_APPLICATION = """

    SELECT 
    APLI.APLI_ID
    ,APLI.APLI_ANO
    ,APLI.APLI_CLAVE
    ,INCO.INCO_ID
    ,APRE.APRE_ID
    ,APRD.APRD_CADENA
    FROM APPDCGRL.GRL_APLICACION APLI
    INNER JOIN APPDCGRL.CFG_INSTRUMENTO_CONFIGURACION INCO ON INCO.INCO_ID = APLI.INCO_ID
    INNER JOIN APPDCGRL.GRL_INSTRUMENTO INTR ON INTR.INTR_ID = INCO.INTR_ID
    INNER JOIN APPDCGRL.GRL_INSTRUMENTO_FAMILIA INFA ON INFA.INFA_ID = INTR.INFA_ID
    INNER JOIN APPDCGRL.GRL_APLICACION_RESULTADO APRE ON APRE.APLI_ID = APLI.APLI_ID
    INNER JOIN APPDCGRL.GRL_APLICACION_RESULTADO_DETALLE APRD ON APRD.APRE_ID = APRE.APRE_ID                    
    WHERE 1=1
    AND APLI_ANO = 2019
    AND APLI_CLAVE IN ('074304312')
    -- AND ROWNUM < 2
    UNION ALL
    SELECT 
    APLI.APLI_ID
    ,APLI.APLI_ANO
    ,APLI.APLI_CLAVE
    ,INCO.INCO_ID
    ,APRE.APRE_ID
    ,APRD.APRD_CADENA
    FROM APPDCGRL.GRL_APLICACION APLI
    INNER JOIN APPDCGRL.CFG_INSTRUMENTO_CONFIGURACION INCO ON INCO.INCO_ID = APLI.INCO_ID
    INNER JOIN APPDCGRL.GRL_INSTRUMENTO INTR ON INTR.INTR_ID = INCO.INTR_ID
    INNER JOIN APPDCGRL.GRL_INSTRUMENTO_FAMILIA INFA ON INFA.INFA_ID = INTR.INFA_ID
    INNER JOIN APPDCGRL.GRL_APLICACION_RESULTADO APRE ON APRE.APLI_ID = APLI.APLI_ID
    INNER JOIN APPDCGRL.GRL_APLICACION_RESULTADO_DETALLE APRD ON APRD.APRE_ID = APRE.APRE_ID                    
    WHERE 1=1
    AND APLI_ANO = 2019
    AND APLI_CLAVE IN ('058404312')
    -- AND ROWNUM < 2

"""

GET_DATA_DEFINITION = "SELECT DICC.*,(VARI_ACUMULADO-VARI_LONGITUD) AS VARI_INICIO FROM APPDCGRL.VW_DICCIONARIOS DICC WHERE 1=1"

class Application:

    # def __init__(self, df_strings, df_splits):
        # self.df_strings = df_strings
        # self.df_splits = df_splits
        # self.splitter = StringSplitter(df_splits)

    def perform_split(self):
        return self.splitter.parallel_split(self.df_strings)

    def data_building(self, year=2019, client="", family="EXANI", examination="EXANI-II", application_num="074304312"):

        # We added the file location because jupyter and py script got differente locations
        # print("Directorio : ",Path(__file__).parent/"database.ini")
        databse_filepath = Path(__file__).parent/"database.ini"
        db_connection = DB_Conn(db_connection=databse_filepath,db_type="oracle")
        db_connection.connect()    

        df_application = self.data_application(db_connection)

        # We got the unique inco_id to avoid to get all definitions
        definition_id = df_application["INCO_ID"].unique()
        df_definition = self.data_definition(db_connection, definition_id)

        df_appdef = pd.merge(df_application, df_definition, how="inner", on=["INCO_ID"])
        logger.log_event("Cantidad de registros del Merge : " + str(len(df_appdef)))
        # print(df_appdef)

        df_strings = df_application[["INCO_ID","APRD_CADENA"]] # df_appdef[["APRD_CADENA"]]
        df_splits = df_definition[["INCO_ID","VARI_NOMBRE","VARI_INICIO","VARI_ACUMULADO"]] # df_appdef[["VARI_NOMBRE","VARI_INICIO","VARI_ACUMULADO"]]
        # print(df_strings)
        # print(df_splits)
        
        
        splitter = StringSplitter(df_splits)
        dfseparado = splitter.parallel_split(df_strings)
        # print(dfseparado)
        dfseparado.to_csv("archivosep.csv", encoding="utf-8")

        return df_appdef

    def data_application(self, db_connection):
        
        loader = Loader("Conexion a BD Califica para recuperar aplicaciones","Aplicaciones recuperadas correctamente").start()        
        df_data_application = db_connection.select_query(GET_DATA_APPLICATION)
        # print(df_data_application)
        loader.stop()

        return df_data_application

    def data_definition(self, db_connection, definition_id):
        
        loader = Loader("Conexion a BD Califica para recuperar diccionario de datos","Diccionarios recuperados correctamente").start()
        values_search = "(" + ','.join([f"(\'{x}\', 0)" for x in definition_id]) + ")"        #We use tuples to avoid max limit of oracle
        final_qry_definition = "%s%s" % (GET_DATA_DEFINITION, " AND (DICC.INCO_ID, 0) IN {} ORDER BY INCO_ID".format(values_search))
        logger.log_event(final_qry_definition)        
        df_data_definition = db_connection.select_query(final_qry_definition)
        # print(df_data_application)
        loader.stop()

        return df_data_definition


if __name__ == '__main__':
    
    logger = Logger()
    application_building = Application()
    start_time = time.time()    
    logger.log_event(message="{} Inicia recuperación de datos BD Califica".format(start_time))
    
    application_building.data_building()

    end_time = time.time()
    logger.log_event(message="{} Concluye recuperación de datos BD Califica".format(end_time))
    
    