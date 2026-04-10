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

# ============================================================
# PASO 0 — Tarea principal que se encola en Redis
# ============================================================

def tarea_entrenamiento(user_id: int, granularidad: str) -> dict:
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
                forecast_df = _modelo_ensemble(serie)
            else:
                forecast_df = _modelo_moving_average(serie)

            _persistir(forecast_df, clave, user_id, granularidad, proporciones)

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
            func.sum(Sale.qty_sold).label('y')   # por si quedaron dupes(duplicados) post-ETL: sumamos ventas del mismo SKU-tienda-fecha (aunque idealmente no debería haber)
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
    df = df.reindex(idx, fill_value=0).reset_index()
    df.columns = ['ds', 'y']
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

    if total_dias >= UMBRAL_DIAS_ENSEMBLE and dias_con_venta >= UMBRAL_DIAS_VENTA:
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


# ============================================================
# MODELOS DE PRONÓSTICO y PERSISTENCIA (guardado en DB)

def _modelo_ensemble(serie: pd.DataFrame) -> pd.DataFrame:
    """Prophet + Random Forest sobre residuales. Paso 3."""
    raise NotImplementedError("Paso 3 pendiente")


def _modelo_moving_average(serie: pd.DataFrame) -> pd.DataFrame:
    """Fallback para series cortas. Paso 2."""
    raise NotImplementedError("Paso 2 pendiente")


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