# Para funciones pesadas de Prophet.
from prophet import Prophet
import pandas as pd

import time

def generate_forecast(df_sales):
    """
    Docstring for generate_forecast
    
    :param df_sales: Description
    Recibe un DataFrame con columnas 'ds' (fechas) y 'y' (ventas) para generar un pronóstico de ventas utilizando Prophet.
    Retorna el forecast para los próximos 30 días.
    """

    model = Prophet(interval_width=0.95, daily_seasonality=True)
    model.fit(df_sales)

    future = model.make_future_dataframe(periods=30)
    forecast = model.predict(future)

    # Retorna solo las columnas relevantes del forecast para  ahorrar memoria en la DB/Cache
    return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_dict()




# Tarea de prueba para simular un proceso pesado en segundo plano, como el entrenamiento de Prophet o Random Forest.

def tarea_de_prueba(user_id):
    """Una tarea simulada que tarda 10 segundos en terminar."""
    print(f"[{user_id}] ⏳ Iniciando cálculo de pronóstico pesado...")
    
    # Simulamos que estamos entrenando Prophet y Random Forest
    time.sleep(10) 
    
    print(f"[{user_id}] ✅ Pronóstico terminado y guardado en la Base de Datos.")
    return True