import duckdb
import os

# Rutas de datos (Asegúrate que Airflow.db esté fuera de raw/analytics)
DB_PATH = os.path.join(os.getcwd(), 'data', 'seismic_events.duckdb')
RAW_PATH = os.path.join(os.getcwd(), 'data', 'raw', 'seismic_raw_events.csv')
RAW_TABLE = 'raw_data_seismic_events'
ANALYTICS_TABLE = 'analytics_seismic_risk'

def run_elt_pipeline():
    """Simula la Carga (L) y la Transformación (T) ejecutada por Airflow."""
    con = duckdb.connect(database=DB_PATH)
    
    print(f"--- 1. LOAD: Cargando datos crudos de {RAW_PATH} ---")
    
    # 1. Carga (L) - Cargar datos CRUDOS a la tabla RAW (Preservando errores/tipos)
    con.execute(f"""
        CREATE OR REPLACE TABLE {RAW_TABLE} (
            event_id VARCHAR,
            time VARCHAR,
            latitude VARCHAR,
            longitude VARCHAR,
            magnitude_raw VARCHAR,  -- CLAVE: Se mantiene como VARCHAR
            depth_km FLOAT,
            tsunami_flag INTEGER
        );
    """)
    con.execute(f"INSERT INTO {RAW_TABLE} SELECT * FROM read_csv_auto('{RAW_PATH}');")
    print(f"✅ Datos crudos cargados en la tabla DuckDB: {RAW_TABLE}")
    
    print("--- 2. TRANSFORM: Aplicando limpieza y agregación (T) ---")
    
    # 2. Transformación (T) - SQL para limpiar, enriquecer y agregar
    sql_transform = f"""
    CREATE OR REPLACE TABLE {ANALYTICS_TABLE} AS
    WITH cleaned_seismic AS (
        SELECT
            event_id,
            TRY_CAST(time AS TIMESTAMP) AS event_time_utc,
            TRY_CAST(latitude AS FLOAT) AS latitude,
            TRY_CAST(longitude AS FLOAT) AS longitude,
            
            -- Limpieza de magnitud: Quita 'M', luego intenta convertir a FLOAT
            TRY_CAST(REPLACE(magnitude_raw, 'M', '') AS FLOAT) AS magnitude_clean,
            depth_km,
            CAST(tsunami_flag AS BOOLEAN) AS tsunami_alert
        FROM {RAW_TABLE}
        -- Filtra nulos o valores no numéricos en columnas críticas
        WHERE magnitude_clean IS NOT NULL AND latitude IS NOT NULL
    ),
    feature_engineering AS (
        SELECT
            *,
            -- Feature: Nivel de Riesgo (Risk_Level)
            CASE
                WHEN magnitude_clean >= 6.0 THEN 'High_Risk'
                WHEN magnitude_clean >= 4.0 THEN 'Medium_Risk'
                ELSE 'Low_Risk'
            END AS risk_level,
            -- Feature: Zona de Localización
            CASE
                WHEN latitude > 30 AND longitude < -100 THEN 'Zone_Pacific_North'
                WHEN latitude < 30 AND longitude < -100 THEN 'Zone_Pacific_South'
                ELSE 'Zone_Continental'
            END AS location_zone
            
        FROM cleaned_seismic
    )
    -- Agregación para Dashboard (Datos Diarios/Por Zona)
    SELECT
        DATE_TRUNC('day', event_time_utc) AS event_day,
        location_zone,
        risk_level,
        COUNT(event_id) AS total_events,
        AVG(magnitude_clean) AS avg_magnitude,
        SUM(CASE WHEN tsunami_alert = TRUE THEN 1 ELSE 0 END) AS tsunami_alerts_count
    FROM feature_engineering
    GROUP BY 1, 2, 3;
    """
    
    con.execute(sql_transform)
    print(f"✅ Transformación completada. Tabla ANALYTICS creada: {ANALYTICS_TABLE}")

    # Opcional: Guardar la tabla ANALYTICS como CSV para el dashboard si es necesario
    analytics_csv_path = os.path.join(os.getcwd(), 'data', 'analytics', 'seismic_analytics.csv')
    os.makedirs(os.path.dirname(analytics_csv_path), exist_ok=True)
    con.execute(f"COPY {ANALYTICS_TABLE} TO '{analytics_csv_path}' (HEADER, DELIMITER ',');")
    print(f"✅ CSV de Analytics generado en: {analytics_csv_path}")

    con.close()

if __name__ == '__main__':
    run_elt_pipeline()