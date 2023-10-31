import requests

import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType, StringType, LongType, DecimalType

from config import db_path, development

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

def insert_data_to_logs(response, conn):
    
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

def get_dtypes():
    
    dbDataFrame = (
        spark.read.format("jdbc")
        .option("url", "jdbc:sqlite:" + db_path)
        .option("dbtable", "history")
        .option("driver", "org.sqlite.JDBC")
        .load()
    )
    
    # From spark column type  
    conversion_d = {
        'integer': int,
        'long': int,
        'string':str,
        'float':float,
        'decimal':float
    }
    
    schema = dbDataFrame.schema
    
    # Get dtypes, excluding id column
    # 1. Get list of booleans. If column name == 'id', then element is True
    list_is_id_b = list(map(lambda col: col.name == 'id', schema))
    
    # 2. Get index of True (index of column id)
    idx_id = [ii for ii, boolean in enumerate(list_is_id_b) if boolean]
    
    # 3. Get dtypes, excluding id column
    dtypes = [conversion_d[column.dataType.typeName()] for ii, column in enumerate(schema) if ii not in idx_id]
    
    print(dtypes)
    return dtypes

def process_api_data_to_spark_dataframe(response, dtypes, spark):
    
    # Get schema
    # For that, get DataFrame for historic data and get its schema
    # This is lazy, so "the entire" table is not read until a spark action is
    #  performed
    
    data_list_of_jsons = response.json()['ListaEESSPrecio']
    
    sc = spark.sparkContext
    
    rdd = sc.parallelize(data_list_of_jsons)

    rdd = rdd.map(lambda row_dict: process_row(row_dict, dtypes))
    rdd.take(3)


    
def process_row(row_dict, dtypes):
    """
    row_dict: list. Each element is a dictionary of feature name / value pairs
    """
    
    prcsd_values_l = list()
    for value, dtype in zip(row_dict.values(), dtypes):
        
        if dtype == float:
            value = value.replace(',', '.')
            value = None if value == '' else value
        
        try:
            value = dtype(value)
        except ValueError:
            print(value)
            print(dtype)
            assert False, ''
        
        prcsd_values_l.append(dtype(value) if value else None)
        
    return prcsd_values_l


if __name__ == '__main__':

    # Call government api to get current oil prices (external data updated every 30min)
    response = get_api_response()
    
    # Get connection to database
    conn = sqlite3.connect(db_path)
    
    # Insert log data from api connection to db: datetime, nota (note) and status
    insert_data_to_logs(response, conn) # Log extraction info to db

    # Get SparkSession
    spark = get_spark_session()
    
    # Process api data, setting the proper dtypes, load to a spark dataframe
    # Processing is done using Spark's RDDs, thus parallelizing the computation
    dtypes = get_dtypes()
    df = process_api_data_to_spark_dataframe(response, dtypes, spark)
    print(df.show(4))
    
    # Close session
    
    # # Check data in logs table
    # c = conn.cursor()
    # c.execute('select * from logs')
    # print(c.fetchall())
    # c.close()
    
    
    if development:
        c = conn.cursor()
        c.execute('DELETE FROM logs;')
        c.close()
    
    
    # Commit the changes
    conn.commit()
        
    # Close the cursor and the connection when you're done
    c.close()
    conn.close()