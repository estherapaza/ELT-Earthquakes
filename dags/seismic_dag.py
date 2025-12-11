from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(
    dag_id='elt_seismic_risk_pipeline',
    start_date=datetime(2025, 12, 1),
    schedule_interval=timedelta(hours=6),      # Scheduling: Ejecución cada 6 horas
    catchup=False,
    tags=['elt', 'geospatial', 'disaster_risk'],
    default_args={
        'retries': 3,                          # Manejo de errores: Reintentos de tareas
        'retry_delay': timedelta(minutes=5),
        'priority_weight': 100,
    }
)
def seismic_elt_pipeline():
    # NOTA: En la implementación real, estos tasks llamarían a los scripts en 'scripts/'

    @task
    def extract_data_from_api():
        """E - Extrae los datos crudos (seismic_raw_events.csv)."""
        # Simulamos la llamada a la función generate_synthetic_data()
        return "data/raw/seismic_raw_events.csv"

    @task(pool='database_pool')
    def load_raw_to_duckdb(file_path: str):
        """L - Carga el CSV crudo directamente a la tabla raw_data_seismic_events de DuckDB (sin limpieza)."""
        # Nota de escalabilidad: Uso de un pool para limitar la concurrencia a la DB.
        print(f"Cargando datos crudos desde {file_path}...")
        return "raw_data_seismic_events"

    @task(trigger_rule='all_success')
    def transform_and_aggregate():
        """T - Ejecuta el SQL de transformación en la DB para crear la tabla analytics_seismic_risk."""
        # Aplicamos: Limpieza de magnitud, corrección de tipos, creación de Risk_Level, y agregación diaria.
        # CRÍTICO: Esta transformación NO toca la tabla raw.
        print("Ejecutando SQL push-down para la transformación...")
        return "analytics_seismic_risk"

    # Definición del flujo: E -> L -> T
    raw_file = extract_data_from_api()
    raw_table = load_raw_to_duckdb(raw_file)
    transform_and_aggregate()

    raw_file >> raw_table >> transform_and_aggregate

seismic_elt_pipeline()
