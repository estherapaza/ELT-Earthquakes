import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_synthetic_data(num_rows=5000):
    """Generates synthetic seismic event data, including intentional raw errors."""
    
    # Base data generation
    start_time = datetime(2025, 11, 1, 0, 0)
    timestamps = [start_time + timedelta(hours=i % (24 * 30)) for i in range(num_rows)]
    
    df = pd.DataFrame({
        'event_id': [f'EVT{i:05d}' for i in range(num_rows)],
        'time': [t.isoformat() + 'Z' for t in timestamps],
        'latitude': np.random.uniform(low=-40, high=40, size=num_rows),
        'longitude': np.random.uniform(low=-120, high=120, size=num_rows),
        'magnitude_raw': np.random.normal(loc=4.5, scale=1.5, size=num_rows), 
        'depth_km': np.random.uniform(low=1, high=700, size=num_rows).round(2),
        'tsunami_flag': np.random.choice([0, 1], num_rows, p=[0.9, 0.1]),
    })
    
    # Introduce RAW Data Errors (CLAVE ELT)
    # A. Magnitude como string o nulo
    df['magnitude_raw'] = df['magnitude_raw'].apply(lambda x: f"{x:.1f}M" if np.random.rand() < 0.1 else f"{x:.1f}")
    df.loc[df.index % 15 == 0, 'magnitude_raw'] = 'N/A' 
    
    # B. Latitud/Longitud como strings, y algunos nulos
    df['latitude'] = df['latitude'].astype(str)
    df['longitude'] = df['longitude'].astype(str)
    df.loc[df.index % 50 == 0, 'latitude'] = '' 
    
    # Guardar en la carpeta RAW
    raw_path = os.path.join(os.getcwd(), 'data', 'raw', 'seismic_raw_events.csv')
    os.makedirs(os.path.dirname(raw_path), exist_ok=True)
    df.to_csv(raw_path, index=False)
    
    print(f"✅ Extracción completada. Datos crudos guardados en: {raw_path}")
    return raw_path

if __name__ == '__main__':
    generate_synthetic_data()