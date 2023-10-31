import requests
import os

import sqlite3
from pyspark.sql import SparkSession

from config import db_path
from schema import schema, dtypes

def get_api_response():
    api_url = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/"
    #api_url = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestresHist/29-10-2023#response-json"
    endpoint = 'estacionesTerrestres/'
    filter = ''
    response = requests.get(api_url + endpoint + filter)
    return response

def process_nota(nota):
    nota_typical_value = 'Archivo de todos los productos en todas las estaciones de servicio. La actualización de precios se realiza cada media hora, con los precios en vigor en ese momento.'
    nota = nota if nota != nota_typical_value else None
    
    
def with_sqlite_connection(db_path):
    def decorator(func):
        def wrapper(*args, **kwargs):
            conn = sqlite3.connect(db_path)
            try:
                result = func(conn, *args, **kwargs)
                conn.commit()  # Commit any changes to the database
                return result
            except Exception as e:
                conn.rollback()  # Rollback changes if an exception occurs
                raise e
            finally:
                conn.close()  # Close the connection, no matter what

        return wrapper

    return decorator

@with_sqlite_connection(db_path)
def insert_data_to_logs(conn, response):
    
    # Get values from response    
    fecha = response.json()['Fecha']
    nota = process_nota(response.json()['Nota'])     # Do not store nota if it has the typical value (store None instead)
    resultado = response.json()['ResultadoConsulta']
    
    # Define sql statement and the values to include
    sql = '''
        INSERT INTO logs (datetime, nota, resultado_consulta)
        VALUES(?, ?, ?);
    '''
    
    values = (fecha, nota, resultado)
    
    # Execute sql statement
    c = conn.cursor()
    c.execute(sql, values)
    
    
def get_spark_session():
    
    spark = (
        SparkSession
        .builder
        .master('local')
        .appName('OilPrice')
        .getOrCreate()
        )
    
    return spark

def process_api_data_to_spark_dataframe(response, dtypes, spark):
    
    # Get schema
    # For that, get DataFrame for historic data and get its schema
    # This is lazy, so "the entire" table is not read until a spark action is
    #  performed
    
    data_list_of_jsons = response.json()['ListaEESSPrecio']
    
    sc = spark.sparkContext
    
    rdd = sc.parallelize(data_list_of_jsons)

    rdd = rdd.map(lambda row_dict: process_row(row_dict, dtypes))
    
    df = spark.createDataFrame(rdd, schema = schema)

    return df



def process_row(row_dict, dtypes):
    """
    row_dict: list. Each element is a dictionary of feature name / value pairs
    """
    
    prcsd_values_l = list()
    for value, dtype in zip(row_dict.values(), dtypes):
        
        if dtype == float:
            value = value.replace(',', '.')
            value = None if value == '' else value
        
        prcsd_values_l.append(dtype(value) if value else None)
        
    return prcsd_values_l


if __name__ == '__main__':

    # Call government api to get current oil prices (external data updated every 30min)
    response = get_api_response()
    
    # Insert log data from api connection to db: datetime, nota (note) and status
    insert_data_to_logs(response) # Log extraction info to db

    # Get SparkSession
    spark = get_spark_session()
    
    # Process api data, setting the proper dtypes, load to a spark dataframe
    # Processing is done using Spark's RDDs, thus parallelizing the computation
    df = process_api_data_to_spark_dataframe(response, dtypes, spark)    
    
    # Save data to db
    (
        df.write.format("jdbc")
        .mode('overwrite')
        .option("url", "jdbc:sqlite:" + os.path.join(os.getcwd(), db_path))
        .option("dbtable", "history")
        .option("driver", "org.sqlite.JDBC")
        .save()
    )