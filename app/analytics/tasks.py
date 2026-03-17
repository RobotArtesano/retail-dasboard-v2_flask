# Para funciones pesadas de Prophet.
from prophet import Prophet
import pandas as pd

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