import cx_Oracle
import pandas as pd
from utils.logger import Logger
from db_connection import DB_Conn
import time
import os
import sys
from pathlib import Path
from utils.loader import Loader

GET_DATA_APPLICATION = """

    SELECT 
    APLI.APLI_ID
    ,APLI.APLI_ANO
    ,APLI.APLI_CLAVE
    ,INCO.INCO_ID
    -- ,DICC.INTR_CLAVE_EXAMEN
    -- ,DICC.INTR_ACRONIMO
    -- ,DICC.SECC_ID
    -- ,DICC.SECC_NOMBRE
    -- ,DICC.INVA_POSICION
    -- ,DICC.VARI_ID
    -- ,DICC.VARI_NOMBRE
    -- ,DICC.VARI_LONGITUD
    -- ,DICC.VARI_PRECISION
    -- ,DICC.VARI_DERIVACION
    -- ,DICC.VARI_ACUMULADO
    ,APRE.APRE_ID
    ,APRD.APRD_CADENA
    FROM APPDCGRL.GRL_APLICACION APLI
    INNER JOIN APPDCGRL.CFG_INSTRUMENTO_CONFIGURACION INCO ON INCO.INCO_ID = APLI.INCO_ID
    INNER JOIN APPDCGRL.GRL_INSTRUMENTO INTR ON INTR.INTR_ID = INCO.INTR_ID
    INNER JOIN APPDCGRL.GRL_INSTRUMENTO_FAMILIA INFA ON INFA.INFA_ID = INTR.INFA_ID
    INNER JOIN APPDCGRL.GRL_APLICACION_RESULTADO APRE ON APRE.APLI_ID = APLI.APLI_ID
    INNER JOIN APPDCGRL.GRL_APLICACION_RESULTADO_DETALLE APRD ON APRD.APRE_ID = APRE.APRE_ID                    
    -- INNER JOIN APPDCGRL.VW_DICCIONARIOS DICC ON DICC.INCO_ID = INCO.INCO_ID
    WHERE 1=1
    AND APLI_ANO = 2019
    AND APLI_CLAVE = '074304312'

"""

GET_DATA_DEFINITION = "SELECT * FROM APPDCGRL.VW_DICCIONARIOS DICC WHERE 1=1"

def data_building(year=2019, client="", family="EXANI", examination="EXANI-II", application_num="074304312"):

    # We added the file location because jupyter and py script got differente locations
    # print("Directorio : ",Path(__file__).parent/"database.ini")
    databse_filepath = Path(__file__).parent/"database.ini"
    db_connection = DB_Conn(db_connection=databse_filepath,db_type="oracle")
    db_connection.connect()    

    df_application = data_application(db_connection)

    # We got the unique inco_id to avoid to get all definitions
    definition_id = df_application["INCO_ID"].unique()
    df_definition = data_definition(db_connection, definition_id)

    df_appdef = pd.merge(df_application, df_definition, how="inner", on=["INCO_ID"])
    logger.log_event("Cantidad de registros del Merge :" + str(len(df_appdef)))

    print(df_appdef)

    return df_appdef

def data_application(db_connection):
    
    loader = Loader("Conexion a BD Califica para recuperar aplicaciones","Aplicaciones recuperadas correctamente").start()


    
    df_data_application = db_connection.select_query(GET_DATA_APPLICATION)
    # print(df_data_application)

    loader.stop()

    return df_data_application

def data_definition(db_connection, definition_id):
    
    loader = Loader("Conexion a BD Califica para recuperar diccionario de datos","Diccionarios recuperados correctamente").start()

    
    final_qry_definition = "%s%s" % (GET_DATA_DEFINITION, " AND DICC.INCO_ID IN ({})".format(str(118)))
    logger.log_event(final_qry_definition)
    
    df_data_definition = db_connection.select_query(final_qry_definition)
    # print(df_data_application)

    loader.stop()

    return df_data_definition


if __name__ == '__main__':
    
    logger = Logger()
    start_time = time.time()    
    logger.log_event(message="{} Inicia recuperación de datos BD Califica".format(start_time))
    
    data_building()

    end_time = time.time()
    logger.log_event(message="{} Concluye recuperación de datos BD Califica".format(end_time))
    
    