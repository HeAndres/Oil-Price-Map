import requests

import sqlite3

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


if __name__ == '__main__':
    
    response = get_api_response()
    conn = sqlite3.connect(db_path)
    
    insert_data_to_logs(response, conn) # Log extraction info to db
    #insert_data_to_logs(response, conn) # Log extraction info to db

    
    
    
    
    
    
    
    
    
    
    
    
    
    # Check data in logs table
    c = conn.cursor()
    c.execute('select * from logs')
    print(c.fetchall())
    c.close()
    
    
    if development:
        c = conn.cursor()
        c.execute('DELETE FROM logs;')
        c.close()
    
    
    # Commit the changes
    conn.commit()
        
    # Close the cursor and the connection when you're done
    c.close()
    conn.close()