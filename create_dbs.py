import sqlite3
from config import db_path

conn = sqlite3.connect(db_path)
c = conn.cursor()

c.execute(
    '''
    CREATE TABLE IF NOT EXISTS logs(
        extraction_id INTEGER PRIMARY KEY NOT NULL,
        datetime DATETIME NOT NULL,
        nota text,
        resultado_consulta text
    );
    '''
)

c.execute(
    '''
    CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY,
        cp INTEGER,
        direccion TEXT,
        horario TEXT,
        latitud FLOAT,
        localidad TEXT NOT NULL,
        longitud_wgs84 FLOAT NOT NULL,
        margen TEXT,
        municipio TEXT,
        precio_biodiesel FLOAT,
        precio_bioetanol FLOAT,
        precio_gas_natural_comprimido FLOAT,
        precio_gas_natural_licuado FLOAT,
        precio_gases_licuados_del_petroleo FLOAT,
        precio_gasoleo_a FLOAT,
        precio_gasoleo_b FLOAT,
        precio_gasoleo_premium FLOAT,
        precio_gasolina_95_e10 FLOAT,
        precio_gasolina_95_e5 FLOAT,
        precio_gasolina_95_e5_premium FLOAT,
        precio_gasolina_98_e10 FLOAT,
        precio_gasolina_98_e5 FLOAT,
        precio_hidrogeno FLOAT,
        provincia TEXT,
        remision TEXT,
        rotulo TEXT,
        tipo_venta TEXT,
        percent_bioetanol FLOAT,
        percent_ester_metilico FLOAT,
        ideess INTEGER NOT NULL,
        idmunicipio INTEGER,
        idprovincia INTEGER,
        idccaa INTEGER
);

    '''
)