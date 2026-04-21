# analytics/tasks.py
import pandas as pd
import numpy as np
from app import db
from app.models import Sale, Product, Inventory, ForecastResult
from sqlalchemy import func
from rq import get_current_job # Para escribir progreso en job.meta

# ============================================================
# CONSTANTES
# ============================================================
UMBRAL_DIAS_ENSEMBLE = 120   # mínimo para Prophet + RF
UMBRAL_DIAS_VENTA    = 30    # mínimo de días con venta > 0

PROPHET_PARAMS = {
    'weekly_seasonality'     : True,
    'yearly_seasonality'     : 'auto',
    'daily_seasonality'      : False,
    'changepoint_prior_scale': 0.05,
}

RF_PARAMS = {
    'n_estimators'    : 100,
    'max_depth'       : 5,
    'min_samples_leaf': 5,
    'random_state'    : 42,
    'n_jobs'          : -1,
}
# Si más del 70% de los días son 0, consideramos la serie como intermitente y aplicamos el modelo de respaldo (moving average simple) en lugar del ensemble.
UMBRAL_CEROS = 0.70   

# ============================================================
# PASO 0 — Tarea principal que se encola en Redis
# ============================================================

def tarea_entrenamiento(user_id: int, granularidad: str, horizonte: int = 30) -> dict:
    """
    Punto de entrada para Redis/RQ. Esta función se encola como tarea en Redis y se ejecuta en segundo plano.
    Orquesta: extraccion -> clasificacion -> modelo -> persistencia(guardado).
    Escribe progreso en job.meta para que el frontend pueda mostrar el estado de la tarea(polling).
    """
    job = get_current_job()

    def _progreso(pct: int, msg:str):
        """"Escribe progreso en el meta del job para polling."""
        if job:
            job.meta['progress'] = pct
            job.meta['message'] = msg
            job.save_meta()

    _progreso(5, 'Extrayendo series de tiempo...')
    series = extraer_series(user_id, granularidad)
    proporciones = calcular_proporciones_tienda(user_id) if granularidad == 'global' else {}

    if not series:
        return {'OK': False, 'error': 'Sin datos suficientes para entrenar.'}

    total = len(series)
    errores = []
    resumen = {'ensemble': 0, 'moving_average': 0}

    # Borramos pronósticos previos de este usuario para este nivel de granularidad (si existen), para evitar confusión con resultados antiguos.
    ForecastResult.query.filter_by(user_id=user_id).delete()
    db.session.commit()

    # Ciclo por cada serie extraída, clasificamos la estrategia, entrenamos el modelo correspondiente y persistimos resultados.

    for i, (clave, serie) in enumerate(series.items(), start=1):
        # Distribuimos el progeso: 5% extraccion + 5% -> 90% modelado -> 100% final y persistencia(gurdado en DB)
        pct = 5 + int((i / total) * 85)
        _progreso(pct, f'Modelando {i}/{total}: {clave}...')


        estrategia = clasificar_serie(serie)
        # Resumimos cuántas series se clasificaron en cada estrategia para mostrar al final.
        resumen[estrategia] += 1

        try:
            if estrategia == 'ensemble':
                forecast_df = _modelo_ensemble(serie, horizonte)
                metricas = _metricas_ensemble(serie)
            else:
                forecast_df = _modelo_moving_average(serie, horizonte)
                metricas = _metricas_moving_average(serie)

            _persistir(forecast_df, clave, user_id, granularidad, proporciones, metricas)

        except Exception as e:
            errores.append(f'{clave}: {str(e)}')

    _progreso(100, f'Listo. {resumen["ensemble"]} ensemble, {resumen["moving_average"]} moving average.')
    return {'ok': True, 'resumen': resumen, 'errores': errores}

"""
Output Final:
{
    'ok': True,
    'resumen': {
        'ensemble': 15,
        'moving_average': 5
    },
    'errores': [
        'SKU001__TiendaA: Error al entrenar modelo X',
        'SKU002: Error al persistir resultado en DB'
    ]
}

"""

# ============================================================
# PASO 1 — EXTRACTOR DE SERIES DE TIEMPO
# ============================================================
def extraer_series(user_id: int, granularidad: str) -> dict[str, pd.DataFrame]:
    """
    Extrae las series de tiempo desde la DB y las devuelve limpias,
    listas para Prophet (columnas ds, y). { clave: DataFrame[ds, y] }

    granularidad: 'global'  → una serie por SKU (ventas sumadas de todas las tiendas)
                  'sku_store' → una serie por combinación SKU-tienda

    Retorna un dict:  { "SKU001": DataFrame, "SKU001__TiendaA": DataFrame, ... }
    La clave usa '__' como separador para poder hacer split limpio después.
    """

    assert granularidad in ('global', 'sku_store'), "granularidad inválida"

    # ── Consulta base ──────────────────────────────────────────────
    # Traemos date, sku_code, store, qty_sold para este usuario
    query = (
        db.session.query(
            Sale.date.label('ds'),
            Product.sku_code.label('sku'),
            Sale.store.label('store'),
            func.sum(Sale.qty_sold).label('y'),   # por si quedaron dupes(duplicados) post-ETL: sumamos ventas del mismo SKU-tienda-fecha (aunque idealmente no debería haber)
            # Agregamos precio de venta y precio lista para calcular descuento
            func.avg(Sale.price).label('price_sale'),
            func.avg(Product.list_price).label('price_list'),
        )
        .join(Product, Sale.product_id == Product.id)
        .filter(Sale.user_id == user_id)
        .group_by(Sale.date, Product.sku_code, Sale.store)
        .order_by(Sale.date)
    )

    df_raw = pd.read_sql(query.statement, db.session.bind)

    if df_raw.empty:
        return {}

    df_raw['ds'] = pd.to_datetime(df_raw['ds'])
    df_raw['y']  = pd.to_numeric(df_raw['y'], errors='coerce').fillna(0)

    # ── Agrupación según granularidad y rellenado de calendario ─────────────────────────────
    series = {}

    if granularidad == 'global':
        agrupado = df_raw.groupby(['ds', 'sku'], as_index=False)['y'].sum()
        for sku, grupo in agrupado.groupby('sku'):
            series[sku] = _rellenar_calendario(grupo[['ds', 'y']])

    else:  # sku_store
        agrupado = df_raw.groupby(['ds', 'sku', 'store'], as_index=False)['y'].sum()
        for (sku, store), grupo in agrupado.groupby(['sku', 'store']):
            series[f'{sku}__{store}'] = _rellenar_calendario(grupo[['ds', 'y']])

    return series


def _rellenar_calendario(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dado un DataFrame con columnas [ds, y], genera un rango diario completo entre min(ds) y max(ds), rellenando con 0 los días sin venta.
    Prophet requiere un calendario completo, sin huecos, para funcionar correctamente. Esta función asegura eso.
    Retorna DataFrame con columnas [ds, y] listo para Prophet.
    """
    df = df.set_index('ds').sort_index()
    idx = pd.date_range(start=df.index.min(), end=df.index.max(), freq='D')
    df  = df.reindex(idx)
    
    # Cantidad sin venta = 0
    df['y'] = df['y'].fillna(0)
    
    # Precios: forward-fill (último precio vigente)
    # y luego backward-fill por si los primeros días no tienen precio
    for col in ['price_sale', 'price_list']:
        if col in df.columns:
            df[col] = df[col].ffill().bfill()
    
    df = df.reset_index()
    df.columns.name = None
    return df


# ============================================================
# PASO 1B — SELECTOR DE ESTRATEGIA (fallback logic)
# ============================================================

def clasificar_serie(serie: pd.DataFrame) -> str:
    """
    Decide qué modelo usar para esta serie.
    Retorna: 'ensemble' o 'moving_average'
    """
    total_dias   = len(serie)
    dias_con_venta = int((serie['y'] > 0).sum())
    pct_ceros = 1 - (dias_con_venta / total_dias) if total_dias > 0 else 1

    if total_dias >= UMBRAL_DIAS_ENSEMBLE 
                    and dias_con_venta >= UMBRAL_DIAS_VENTA
                    and pct_ceros < UMBRAL_CEROS:
        return 'ensemble'
    else:
        return 'moving_average'


# ============================================================
# PASO 1C — MAPA PROPORCIONAL (para distribuir top-down)
# ============================================================

def calcular_proporciones_tienda(user_id: int) -> dict[str, dict[str, float]]:
    """
    Calcula qué proporción histórica de ventas corresponde a cada tienda
    para cada SKU. Se usa cuando granularidad='global' para distribuir
    el pronóstico global a nivel tienda.

    Retorna: { "SKU001": {"TiendaA": 0.65, "TiendaB": 0.35}, ... }
    """
    query = (
        db.session.query(
            Product.sku_code.label('sku'),
            Sale.store.label('store'),
            func.sum(Sale.qty_sold).label('total')
        )
        .join(Product, Sale.product_id == Product.id)
        .filter(Sale.user_id == user_id)
        .group_by(Product.sku_code, Sale.store)
    )

    df = pd.read_sql(query.statement, db.session.bind)

    if df.empty:
        return {}

    proporciones = {}
    for sku, grupo in df.groupby('sku'):
        total_sku = grupo['total'].sum()
        if total_sku > 0:
            proporciones[sku] = {
                row['store']: round(row['total'] / total_sku, 4)
                for _, row in grupo.iterrows()
            }

    return proporciones

# ===========================================================
# ===========================================================
# MODELOS DE PRONÓSTICO y PERSISTENCIA (guardado en DB)

def _modelo_ensemble(serie: pd.DataFrame, horizonte: int = 30) -> pd.DataFrame:
    """Prophet + Random Forest sobre residuales.
    
    Orquesta el ensemble secuencial Prophet → construcción de features → Random Forest.
    Pronostico final = pronóstico de Prophet + corrección de RF.
    
     Retorna DataFrame con columnas [ds, yhat, yhat_lower, yhat_upper] con el pronóstico para los próximos 'horizonte' días."""

     # 3A — Entrenamos Prophet
     _, df_hist, df_fut = _entrenar_prophet(serie, horizonte)

     # 3B — Construimos features y entrenamos RF sobre residuales de Prophet
    df_train = _construir_features_historicos(serie, df_hist)
    rf, features_usados = _entrenar_rf(df_train)

    # 3B: Features del futuro (horizonte) — usamos historial real para lags y rolling, no predicciones
    df_fut_features = _construir_features_futuro(df_fut, serie)
    # Prediccion de RF sobre el horizonte futuro
    X_fut = df_fut_features[features_usados].fillna(0)
    rf_correccion = rf.predict(X_fut)
    
    # Ensemble final = Prophet + corrección RF
    df_fut = df_fut.copy()
    df_fut['yhat']       = (df_fut['yhat']       + correccion_rf).clip(lower=0)
    df_fut['yhat_lower'] = (df_fut['yhat_lower'] + correccion_rf).clip(lower=0)
    df_fut['yhat_upper'] = (df_fut['yhat_upper'] + correccion_rf).clip(lower=0)

    return df_fut[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]


def _metricas_ensemble(serie: pd.DataFrame) -> dict:
    """
    Evalúa el ensemble con walk-forward validation.
    Usa los últimos 30 días como test, el resto como train.
    Nunca mezcla datos futuros con el entrenamiento.
    """
    TEST_DIAS = 30

    if len(serie) <= TEST_DIAS + 28:   # 28 por el lag_28 mínimo
        return None

    # ej. Serie de 100 días:
    # train → días 1–70
    # test → días 71–100
    train = serie.iloc[:-TEST_DIAS].copy()
    test  = serie.iloc[-TEST_DIAS:].copy()

    # Entrenamos solo con el train
    _, df_hist_train, _ = _entrenar_prophet(train, horizonte=TEST_DIAS)
    df_train_features   = _construir_features_historicos(train, df_hist_train)
    rf, features_usados = _entrenar_rf(df_train_features)

    # Predecimos el periodo de test usando Prophet entrenado en train
    from prophet import Prophet
    import logging
    logging.getLogger('prophet').setLevel(logging.WARNING)
    logging.getLogger('cmdstanpy').setLevel(logging.WARNING)

    modelo_eval = Prophet(
        weekly_seasonality      = PROPHET_PARAMS['weekly_seasonality'],
        yearly_seasonality      = PROPHET_PARAMS['yearly_seasonality'],
        daily_seasonality       = PROPHET_PARAMS['daily_seasonality'],
        changepoint_prior_scale = PROPHET_PARAMS['changepoint_prior_scale'],
    )
    modelo_eval.fit(train[['ds', 'y']])

    df_test_prophet = modelo_eval.predict(test[['ds']])
    df_test_features = _construir_features_futuro(
        df_test_prophet[['ds', 'yhat', 'yhat_lower', 'yhat_upper']],
        train
    )

    X_test       = df_test_features[features_usados].fillna(0)
    correccion   = rf.predict(X_test)
    yhat_final   = (df_test_prophet['yhat'].values + correccion).clip(min=0)
    reales       = test['y'].values

    # ── Cálculo de métricas ───────────────────────────────────────────────────
    mae  = float(np.mean(np.abs(reales - yhat_final)))
    rmse = float(np.sqrt(np.mean((reales - yhat_final) ** 2)))

    mask_venta = reales > 0
    mape = float(np.mean(np.abs(
        (reales[mask_venta] - yhat_final[mask_venta]) / reales[mask_venta]
    )) * 100) if mask_venta.any() else None

    denominador = (np.abs(reales) + np.abs(yhat_final)) / 2
    mask_den    = denominador > 0
    smape = float(np.mean(np.abs(
        reales[mask_den] - yhat_final[mask_den]
    ) / denominador[mask_den]) * 100) if mask_den.any() else None

    return {
        'modelo' : 'ensemble',
        'mae'    : round(mae,   2),
        'rmse'   : round(rmse,  2),
        'mape'   : round(mape,  2) if mape  is not None else None,
        'smape'  : round(smape, 2) if smape is not None else None
    }



    # ══════════════════════════
    # PASO 3A — PROPHET
    # ══════════════════════════

def _entrenar_prophet(serie: pd.DataFrame, horizonte: int) -> tuple:
    """
    Entrena Prophet sobre la serie histórica y genera el forecast.

    Retorna:
    - modelo   : objeto Prophet entrenado (lo necesitamos para calcular residuales)
    - df_hist  : predicciones sobre el periodo histórico (para calcular residuales)
    - df_fut   : predicciones sobre el horizonte futuro (30/60/90/120 días)
    """
    # import local — Prophet es pesado
    # Tarda en cargar y usa Stan (compilación de modelos bayesianos), por eso lo importamos dentro de la función y no al inicio del archivo.
    # importarlo globalmente ralentiza inicio de la app y también puede generar problemas de importación circular con otros módulos.
    from prophet import Prophet
    import holidays as hols

    

    # Silenciar los logs de Stan que Prophet imprime por defecto
    # Prophet usa cmdstanpy como backend, que a su vez usa Stan. Ambos imprimen logs de info y warning que pueden ser molestos en la consola de Redis. Aquí los silenciamos.
    # setLevel(logging.WARNING) → solo muestra warnings y errores, no info ni debug. Esto limpia la salida de Redis y hace que los logs sean más legibles.
    import logging
    logging.getLogger('prophet').setLevel(logging.WARNING)
    logging.getLogger('cmdstanpy').setLevel(logging.WARNING)

    # ── Construir DataFrame de holidays de México ─────────────────────────────
    # Tomamos el rango de años que cubre la serie + el horizonte futuro
    anio_inicio = serie['ds'].dt.year.min()
    anio_fin    = (serie['ds'].max() + pd.Timedelta(days=horizonte)).year

    mx_holidays = hols.country_holidays('MX', years=range(anio_inicio, anio_fin + 1))

    df_holidays = pd.DataFrame([
        {'ds': pd.Timestamp(fecha), 'holiday': nombre}
        for fecha, nombre in mx_holidays.items()
    ])

    # ── Agregar Buen Fin manualmente ──────────────────────────────────────────
    # La librería holidays no lo incluye porque no es feriado oficial,
    # pero en retail mexicano es el evento más importante del año.
    # Cae el tercer viernes de noviembre — lo calculamos dinámicamente.
    buen_fin_rows = []
    for anio in range(anio_inicio, anio_fin + 1):
        noviembre   = pd.Timestamp(f'{anio}-11-01')
        viernes     = noviembre + pd.offsets.WeekOfMonth(week=2, weekday=4)
        # Agregamos viernes, sábado, domingo y lunes del Buen Fin
        for delta in range(4):
            buen_fin_rows.append({
                'ds'     : viernes + pd.Timedelta(days=delta),
                'holiday': 'Buen Fin'
            })

    df_buen_fin = pd.DataFrame(buen_fin_rows)
    df_holidays = pd.concat([df_holidays, df_buen_fin], ignore_index=True)

    # ── Agregar quincenas como pseudo-holiday ─────────────────────────────────
    # Prophet no modela el ciclo quincenal por defecto.
    # Lo agregamos como evento recurrente para que aprenda el patrón de pago.
    quincenas = []
    for anio in range(anio_inicio, anio_fin + 1):
        for mes in range(1, 13):
            for dia in [1, 15]:
                quincenas.append({
                    'ds'          : pd.Timestamp(f'{anio}-{mes:02d}-{dia:02d}'),
                    'holiday'     : 'quincena',
                    'lower_window': 0,    # el efecto empieza ese día
                    'upper_window': 2     # y dura 2 días después
                })

    df_quincenas = pd.DataFrame(quincenas)
    df_holidays  = pd.concat([df_holidays, df_quincenas], ignore_index=True)

    # ============= Construccion del modelo Prophet con los parámetros definidos en PROPHET_PARAMS ======================
    modelo = Prophet(
        weekly_seasonality       = PROPHET_PARAMS['weekly_seasonality'],
        yearly_seasonality       = PROPHET_PARAMS['yearly_seasonality'],
        daily_seasonality        = PROPHET_PARAMS['daily_seasonality'],
        changepoint_prior_scale  = PROPHET_PARAMS['changepoint_prior_scale'], # que tan sensible a cambios repentinos en la serie (default 0.05, valores más altos = más sensible)
        holidays                 = df_holidays
    )

    # Doble corchete para pasar un DataFrame con solo las columnas ds y y, que es lo que Prophet espera.
    modelo.fit(serie[['ds', 'y']])

    # ── Predicción histórica (para residuales) ────────────────────────────────
    # Usamos las mismas fechas del historial, no fechas nuevas
    df_hist = modelo.predict(serie[['ds']])

    # ── Predicción futura ─────────────────────────────────────────────────────
    futuro  = modelo.make_future_dataframe(periods=horizonte, freq='D')
    df_fut  = modelo.predict(futuro)

    # Solo nos quedamos con las fechas futuras (posteriores al historial)
    # filtrado booleano: df_fut['ds'] > ultima_fecha y seleccionamos solo columnas relevantes para el pronóstico: ds, yhat, yhat_lower, yhat_upper.
    # .copy() para evitar SettingWithCopyWarning de pandas al modificar df_fut después.
    ultima_fecha = serie['ds'].max()
    df_fut = df_fut[df_fut['ds'] > ultima_fecha][['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()

    # Clip: nunca negativo
    # todo valor < 0 -> 0
    for col in ['yhat', 'yhat_lower', 'yhat_upper']:
        df_fut[col] = df_fut[col].clip(lower=0)

    return modelo, df_hist, df_fut
    
# ════════════════════════════════════════════════════════════════════════════════
# PASO 3B — FEATURES DE RANDOM FOREST
# ════════════════════════════════════════════════════════════════════════════════

def _construir_features_historicos(serie: pd.DataFrame, df_hist: pd.DataFrame) -> pd.DataFrame:
    """
    Construye el DataFrame de features sobre el historial real.
    Se usa para entrenar RF sobre los residuales de Prophet.

    Recibe:
    - serie   : DataFrame [ds, y, price_sale, price_list] — historial real
    - df_hist : predicciones de Prophet sobre el mismo periodo

    Retorna DataFrame con todas las features + columna 'residual' como target.
    """
    df = serie.copy()
    df = df.merge(df_hist[['ds', 'yhat']], on='ds', how='left')
    df = df.rename(columns={'yhat': 'yhat_prophet'})

    # ── Residual = lo que Prophet no explicó ─────────────────────────────────
    df['residual'] = df['y'] - df['yhat_prophet']

    # ── Features de calendario ────────────────────────────────────────────────
    # .dt → accesor de pandas para trabajar con columnas de tipo datetime. Permite extraer componentes como día de la semana, mes, etc.
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['is_weekend']  = (df['day_of_week'] >= 5).astype(int)
    df['month']       = df['ds'].dt.month
    df['is_quincena'] = df['ds'].dt.day.isin([1, 2, 14, 15, 16]).astype(int)
    df['is_fin_mes']  = (df['ds'].dt.day >= 28).astype(int)

    # ── Lags seguros (solo lag_28 para no filtrar el futuro) ─────────────────
    # .shift desplaza la serie hacia abajo, creando lags. lag_28 = ventas de hace 28 días. Es un feature clave para capturar estacionalidad mensual.
    df['lag_28'] = df['y'].shift(28)

    # ── Rolling means sobre ventas reales ────────────────────────────────────
    df['rolling_mean_14'] = df['y'].shift(1).rolling(14).mean()
    df['rolling_mean_28'] = df['y'].shift(1).rolling(28).mean()
    df['rolling_std_7']   = df['y'].shift(1).rolling(7).std()

    # ── Features de descuento ─────────────────────────────────────────────────
    # Solo si el usuario tiene precio de lista cargado en el catálogo
    if 'price_sale' in df.columns and 'price_list' in df.columns:
        # Evitamos división por cero si price_list es 0
        mask = df['price_list'] > 0
        df['discount_pct'] = 0.0
        df.loc[mask, 'discount_pct'] = (
            (df.loc[mask, 'price_list'] - df.loc[mask, 'price_sale'])
            / df.loc[mask, 'price_list']
        ).clip(lower=0)   # descuento no puede ser negativo/ .clip(lower, upper) → limita los valores a un rango. Aquí aseguramos que el descuento no sea negativo, aunque idealmente no debería serlo.
        df['is_discount'] = (df['discount_pct'] > 0.01).astype(int)
    else:
        df['discount_pct'] = 0.0
        df['is_discount']  = 0

    # ── Días sin venta (proxy de serie intermitente) ──────────────────────────
    df['days_since_last_sale'] = _calcular_days_since_last_sale(df['y'])

    # ── Eliminamos filas con NaN (producto de lags y rolling) ─────────────────
    # Las primeras 28 filas siempre tendrán NaN en lag_28
    df = df.dropna(subset=['lag_28', 'rolling_mean_14', 'rolling_mean_28'])

    return df


def _calcular_days_since_last_sale(serie_y: pd.Series) -> pd.Series:
    """
    Calcula cuántos días han pasado desde la última venta > 0.
    Útil para detectar series intermitentes o productos inactivos.
    """
    result = []
    contador = 0
    for val in serie_y:
        if val > 0:
            contador = 0
        else:
            contador += 1
        result.append(contador)
    return pd.Series(result, index=serie_y.index)


def _construir_features_futuro(df_fut: pd.DataFrame,
                                serie: pd.DataFrame) -> pd.DataFrame:
    """
    Construye features para el horizonte futuro.
    Los lags y rolling means se calculan sobre el historial real,
    no sobre predicciones — así evitamos el problema de lags futuros.
    """
    df = df_fut.copy()

    # ── Features de calendario (siempre disponibles) ──────────────────────────
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['is_weekend']  = (df['day_of_week'] >= 5).astype(int)
    df['month']       = df['ds'].dt.month
    df['is_quincena'] = df['ds'].dt.day.isin([1, 2, 14, 15, 16]).astype(int)
    df['is_fin_mes']  = (df['ds'].dt.day >= 28).astype(int)

    # ── lag_28: siempre disponible para horizonte <= 120 días ─────────────────
    # Construimos un índice de fechas → ventas reales para hacer lookup
    hist_index = serie.set_index('ds')['y']

    def lag_28_para_fecha(fecha):
        fecha_lag = fecha - pd.Timedelta(days=28)
        return hist_index.get(fecha_lag, 0.0)

    df['lag_28'] = df['ds'].apply(lag_28_para_fecha)

    # ── Rolling means: usamos los últimos valores del historial (estáticos) ───
    # Son la "memoria" del sistema al momento de predecir
    df['rolling_mean_14'] = serie['y'].tail(14).mean()
    df['rolling_mean_28'] = serie['y'].tail(28).mean()
    df['rolling_std_7']   = serie['y'].tail(7).std()

    # ── Descuento: usamos el último valor conocido (forward-fill del historial) 
    if 'price_sale' in serie.columns and 'price_list' in serie.columns:
        ultimo_price_sale = serie['price_sale'].iloc[-1]  # .iloc[-1] → Último valor de la columna
        ultimo_price_list = serie['price_list'].iloc[-1]
        if ultimo_price_list > 0:
            disc = max((ultimo_price_list - ultimo_price_sale) / ultimo_price_list, 0)
        else:
            disc = 0.0
        df['discount_pct'] = disc
        df['is_discount']  = int(disc > 0.05)
    else:
        df['discount_pct'] = 0.0
        df['is_discount']  = 0

    # IMPORTANTE FEATURE: pendiente modelo probabilistico para determiar nivel de inventario necesario para cubrir demanda con cierto nivel de servicio. Mientras tanto, usamos un proxy simple: días desde última venta.
    # ── days_since_last_sale: al inicio del horizonte = días desde última venta
    ultima_venta = serie[serie['y'] > 0]['ds'].max()
    if pd.isna(ultima_venta):
        base_dias = len(serie)
    else:
        base_dias = (serie['ds'].max() - ultima_venta).days

    df['days_since_last_sale'] = [base_dias + i for i in range(len(df))]

    return df


def _entrenar_rf(df_train: pd.DataFrame) -> object:
    """
    Entrena Random Forest sobre los residuales de Prophet.

    El target es 'residual' — lo que Prophet no pudo explicar.
    RF aprende a corregir esos errores sistemáticos usando
    los features de calendario, descuento y memoria histórica.
    """
    from sklearn.ensemble import RandomForestRegressor

    FEATURES = [
        'day_of_week', 'is_weekend', 'month', 'is_quincena', 'is_fin_mes',
        'lag_28', 'rolling_mean_14', 'rolling_mean_28', 'rolling_std_7',
        'discount_pct', 'is_discount',
        'days_since_last_sale',
        'yhat_prophet'
    ]

    # Solo usamos features que existan en el DataFrame
    features_disponibles = [f for f in FEATURES if f in df_train.columns]

    X = df_train[features_disponibles].fillna(0)
    y = df_train['residual'].fillna(0)

    rf = RandomForestRegressor(
        n_estimators    = RF_PARAMS['n_estimators'],
        max_depth       = RF_PARAMS['max_depth'],
        min_samples_leaf= RF_PARAMS['min_samples_leaf'],
        random_state    = RF_PARAMS['random_state'],
        n_jobs          = RF_PARAMS['n_jobs']
    )
    rf.fit(X, y)

    return rf, features_disponibles


# =============================================================
# PASO 2 — MODELO DE RESPALDO PARA SERIES CORTAS (moving average simple)
# =============================================================
def _modelo_moving_average(serie: pd.DataFrame, horizonte: int = 30) -> pd.DataFrame:
    """
    Fallback para series con menos de 120 días o menos de 30 días con venta > 0.
    Usa una media movil ponderada simple como pronostico central.
    Los intervalos sse estiman con +- desviacion estandar de la ventana movil.
    
    Parametros:
    - serie: DataFrame con columnas [ds, y], con calendario completo (sin huecos)
    - horizonte: dias a pronosticar hacia adelante (default: 30)
    
    Retorna DataFrame con columnas [ds, yhat, yhat_lower, yhat_upper] con el pronóstico para los próximos 'horizonte' días.
    """
    VENTANA = 14   # días — captura ciclo quincenal
    # convertimos a numpy para cálculos rápidos
    y = serie['y'].values

    # ── Ventana base: últimos 14 días disponibles ──────────────────────────────
    #[-variable:] es una forma de tomar los últimos 'variable' elementos de un array, sin importar su longitud total.
    ventana_vals = y[-VENTANA:] if len(y) >= VENTANA else y

    # float() para convertir a escalar Python y evitar problemas de serialización JSON al guardar en DB o enviar al frontend.
    media    = float(np.mean(ventana_vals))
    std      = float(np.std(ventana_vals))

    # Nunca pronosticamos negativo
    yhat       = max(media, 0.0)
    yhat_lower = max(media - std, 0.0)
    yhat_upper = max(media + std, 0.0)

    # ── Generamos el horizonte futuro ─────────────────────────────────────────
    # La fecha de inicio es el día siguiente al último día disponible en la serie, para evitar solapamiento.
    # pd.Timedelta(days=1) -> suma un dia.
    # pd.date_range(start=..., periods=..., freq='D') -> genera un rango de fechas
    ultima_fecha = serie['ds'].max()
    fechas_futuras = pd.date_range(
        start = ultima_fecha + pd.Timedelta(days=1),
        periods = horizonte,
        freq = 'D'
    )

    # DataFrame final con el pronóstico para los próximos 'horizonte' días, con columnas ds, yhat, yhat_lower, yhat_upper.
    forecast_df = pd.DataFrame({
        'ds'         : fechas_futuras,
        'yhat'       : yhat,
        'yhat_lower' : yhat_lower,
        'yhat_upper' : yhat_upper
    })

    return forecast_df


# ════════════════════════════════════════════════════════════════════════════════
# PASO 2B — MÉTRICAS PARA MOVING AVERAGE
# ════════════════════════════════════════════════════════════════════════════════

def _metricas_moving_average(serie: pd.DataFrame) -> dict:
    """
    Calcula MAE, MAPE, RMSE y sMAPE usando leave-one-out sobre la ventana histórica.
    Para series cortas hacemos walk-forward mínimo: predecimos cada día
    como la media de los 114 días anteriores y medimos contra el real.

    Retorna dict con las métricas o None si la serie es demasiado corta.
    """
    VENTANA = 14
    y = serie['y'].values

    # Necesitamos al menos VENTANA + 1 días para calcular algo
    if len(y) <= VENTANA:
        return None

    reales    = []
    predichos = []

    """
    VENTANA = 3
    y = [10, 12, 8, 15, 20]

    i = 3 → usa [10,12,8] → predice 15
    i = 4 → usa [12,8,15] → predice 20

    y[a:b]  → incluye a, excluye b
    y = [10,12,8,15,20]
    i = 3
    → y[0:3] = [10,12,8]
    """
    for i in range(VENTANA, len(y)):
        ventana_i  = y[i - VENTANA:i]
        pred_i     = max(float(np.mean(ventana_i)), 0.0)
        reales.append(y[i])
        predichos.append(pred_i)

    # Convertimos a numpy para cálculos vectorizados
    reales    = np.array(reales)
    predichos = np.array(predichos)

    mae  = float(np.mean(np.abs(reales - predichos)))
    rmse = float(np.sqrt(np.mean((reales - predichos) ** 2)))

    # MAPE clásico — excluimos días con venta cero para evitar división por cero
    mask_venta = reales > 0
    mape = float(np.mean(np.abs((reales[mask_venta] - predichos[mask_venta])
                                 / reales[mask_venta])) * 100) if mask_venta.any() else None

    # sMAPE — simétrico, no explota con ceros
    denominador = (np.abs(reales) + np.abs(predichos)) / 2
    mask_den    = denominador > 0
    smape = float(np.mean(np.abs(reales[mask_den] - predichos[mask_den])
                           / denominador[mask_den]) * 100) if mask_den.any() else None

    return {
        'modelo' : 'moving_average',
        'mae'    : round(mae,   2),
        'rmse'   : round(rmse,  2),
        'mape'   : round(mape,  2) if mape  is not None else None,
        'smape'  : round(smape, 2) if smape is not None else None
    }


def _persistir(forecast_df, clave, user_id, granularidad, proporciones):
    """Guarda pronósticos en ForecastResult. Paso 4."""
    raise NotImplementedError("Paso 4 pendiente")



# ============================================================
# Tarea de prueba para simular un proceso pesado en segundo plano, como el entrenamiento de Prophet o Random Forest.
# (sirve para verificar que Redis funciona)

def tarea_de_prueba(user_id):
    """Una tarea simulada que tarda 10 segundos en terminar."""
    print(f"[{user_id}] ⏳ Iniciando cálculo de pronóstico pesado...")
    
    # Simulamos que estamos entrenando Prophet y Random Forest
    time.sleep(10) 
    
    print(f"[{user_id}] ✅ Pronóstico terminado y guardado en la Base de Datos.")
    return True

