import math
import numpy as np
import pandas as pd
from typing import Tuple, List, Optional
import io

# --- CONSTANTES DE COLUMNAS REQUERIDAS ---
REQUIRED_SALES_COLS = ['date', 'sku_code', 'store', 'qty_sold', 'price_sale', 'unit_cost']

REQUIRED_CATALOG_COLS = ['sku_code', 'name', 'price', 'cost']
# Opcionales que el modelo acepta: department, category, brand, supplier, etc.

REQUIRED_INVENTORY_COLS = ['sku_code', 'store', 'inv_qty']
# Opcionales: on_order, safety_stock, etc.

# Diccionario de sinónimos para normalizar las columnas Minimas Requeridas.
SYNONYMS = {
   "store": ["tienda","location","sucursal","branch","punto_venta","pos","punto de venta","puntodeventa","punto deventa","pdv","tiendas","centros","centro",
             "sucursales"],
    "date": ["date","fecha","timestamp","datetime","fecha_venta","fecha de venta","fecha de contabilizacion","fecha_contabilizacion","fecha de contabilizacion",
             "fecha_contabilización","fecha de contabilización","fecha de transacción","fecha_transaccion","fecha de transacción","fecha_transacción",
             "fecha_hora","fecha y hora","fecha de registro","fecha_registro"],
    "sku_code": ["sku","product","producto","item","articulo","codigo", "sku_id","material","modelo","articulo padre","articulo hijo"], # Nuestro modelo usa sku_code

    # Ventas
    "qty_sold": ["unidades","piezas","units","uds","pz","pzas","cantidad","piezas vendidas","qty", "quantity","UM","cantidad_vendida"],
    "price_sale": ["precio_neto","price_net","venta","venta_neta", "precio neto", "price", "precio"],
    "unit_cost": ["cost","costo","costo_tienda","costo_unitario","costo_proveedor"],

    # Catalogo
    "name": ["nombre", "descripcion", "product_name", "product", "producto", "description", "nombre_producto", "producto_nombre"],
    "department": ["department","departamento","category","categoria", "categoria_producto", "departamento_producto"],
    "price": ["price","precio","precio_lista","list_price","precio_lleno", "precio_venta", "precio_sugerido", "precio_referencia", "full_price", "precio_base",
              "precio_catalogo"],
    "cost": ["cost","costo","costo_proveedor","costo_unitario", "costo_producto", "unit_cost", "cost_price", "precio_costo","costo_material"],

    # Inventario
    "inv_qty": ["inventory","stock","inventario","quantity_on_hand","inv","IOH", "cantidad_disponible", "cantidad_inventario", "existencia", "cantidad_existencia",
                "cantidad_stock", "existencias", "stock_disponible", "disponible_stock", "disponible_inventario", "disponible", "on_hand"],
}

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Estandariza los nombres de las columnas usando el diccionario de sinónimos."""
    # Convertimos las columnas del df a minúsculas para facilitar la comparación
    columns_lower = {col.lower().strip(): col for col in df.columns}
    mapping = {}
    
    for canonical_name, aliases in SYNONYMS.items():
        for alias in aliases:
            if alias.lower() in columns_lower:
                # Mapeamos el nombre original al nombre canónico
                mapping[columns_lower[alias.lower()]] = canonical_name
                break # Encontramos la coincidencia para este nombre canónico
                
    return df.rename(columns=mapping)

# Tipado y Firmas (sintaxis typing) para mayor claridad y mantenimiento del código
# se utiliza ":" para las variables y "->" para el retorno de la función:
# file_stream: bytes: indica que el archivo se lee en memoria como bytes, lo cual es común al manejar archivos subidos en Flask/FastAPI
# filename: str: el nombre del archivo subido (el argumento debe ser un texto), que se usa para detectar el formato (csv, xls, json)
# Tuple[Optional[pd.DataFrame], List[str]]: indica que la función retorna una tupla donde el primer elemento es un DataFrame de pandas o None si hubo errores,
#  y el segundo elemento es una lista de strings con los mensajes de error.
def process_upload(file_stream: bytes, filename: str, upload_type: str) -> Tuple[Optional[pd.DataFrame], List[str]]:
    """
        Procesa el archivo subido, detecta formato, normaliza y extrae lo esencial.
        Lee, valida y limpia un archivo CSV de ventas en memoria.

        Lee y valida archivos segun su tipo: 'sales', 'catalog' o 'inventory'.
        
        Parámetros:
        - file_stream: Los bytes del archivo subido (request.files['file'].read())
        
        Retorna:
        - Una tupla: (DataFrame limpio o None si falla y Lista de errores)
    """
    errores = []
    
    try:
        # 1. Detección de Formato
        # io.BytesIO permite tratar los bytes del archivo como un archivo en memoria, lo que es necesario para pandas al leer desde un stream.
        # so se guarda el archivo en el servidor, se procesa directamente en memoria para mayor seguridad y eficiencia.
        ext = filename.rsplit('.', 1)[-1].lower()
        if ext == 'csv':
            df = pd.read_csv(io.BytesIO(file_stream))
        elif ext in ['xls', 'xlsx']:
            df = pd.read_excel(io.BytesIO(file_stream))
        elif ext == 'json':
            df = pd.read_json(io.BytesIO(file_stream), orient='records')
        else:
            return None, ["Formato de archivo no soportado. Use CSV, Excel o JSON."]

        # 2. Normalización de Nomenclaturas del Cliente
        df = normalize_columns(df)
        

        # Enrutador de validacion (Factory Pattern para el ETL)
        if upload_type == 'sales':
            return _validate_sales(df)
        elif upload_type == 'catalog':
            return _validate_catalog(df)
        elif upload_type == 'inventory':
            return _validate_inventory(df)
        else:
            return None, ["Tipo de carga no reconocido. Use informacion de: 'sales', 'catalog' o 'inventory'."]
        
    except pd.errors.EmptyDataError:
        return None, ["El archivo está completamente vacío."]
    except Exception as e:
        return None, [f"Error inesperado al procesar el archivo: {str(e)}"]
    

# Funciones de Validación Específicas por Tipo de Archivo (Sales, Catalog, Inventory)
def _validate_sales(df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], List[str]]:
    errores = []
        
    # Validación de Esquema
    missing_cols = [col for col in REQUIRED_SALES_COLS if col not in df.columns]
    if missing_cols:
        return None,[f"Faltan columnas obligatorias tras la normalización: {', '.join(missing_cols)}"]
            
    #  Descarte de Columnas Extra (Nos quedamos solo con lo que importa)
    df = df[REQUIRED_SALES_COLS].copy()
        
    # Transformación y Limpieza (Tipos de Datos)
        # Convertir 'date' a datetime. errors='coerce' convierte errores en NaT (Not a Time)
        # .any() funciona como un OR gigante: True si almenos un elemento es verdadero (o distinto de NaT en este caso).
        # Si alguna fecha es inválida, isnull() será True para esa fila, y any() detectará que hay al menos un True en la columna.
    df['date'] = pd.to_datetime(df['date'], errors='coerce')     
    if df['date'].isnull().any():
        errores.append("Existen fechas con formato inválido. Utilice el formato AAAA-MM-DD. Revise su archivo.")
            
        # Forzar tipos numéricos. errors='coerce' convierte texto/basura en NaN
    numeric_cols = ['qty_sold', 'price_sale', 'unit_cost']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Asegurarnos de que los SKUs y Tiendas sean cadenas de texto sin espacios extra
    df['sku_code'] = df['sku_code'].astype(str).str.strip()
    df['store'] = df['store'].astype(str).str.strip()
            
        # el primer any() busca en columnas y el segundo any()busca en los resultados de esas columnas.
    if df[numeric_cols].isnull().any().any():
        return None, errores + ["Existen valores de texto o vacíos donde se esperaban números (cantidades o precios). Revise su archivo."]

    # Validaciones de Lógica de Negocio (Retail)
    # Precios y costos no pueden ser negativos
    if (df['price_sale'] < 0).any() or (df['unit_cost'] < 0).any():
            return None, errores + ["Se detectaron precios o costos negativos. Revise su información financiera. Revise su información financiera."]
            
        # Advertencia silenciosa (no bloquea): Si hay devoluciones (qty_sold < 0)
        # Nota de Caplin: Podríamos crear una lista separada de "warnings" si quisieras mostrar alertas
        # en amarillo/dorado en el frontend, pero por ahora lo permitimos.

    # AGRUPACION A NIVEL DIARIO (Ticket -> Diario)
    # Esto reduce draticamente el tamano de los datos y prepara la serie de tiempo para Prophet, que funciona mejor con datos diarios.
    try:
        df = df.groupby(['date', 'sku_code', 'store'], as_index=False).agg({
            'qty_sold': 'sum',  # Sumamos todas las piezas vendidas en el dia
            'price_sale': 'mean',  # Precio promedio de venta por día/sku/tienda
            'unit_cost': 'mean'    # Costo promedio por día/sku/tienda
        })

        # Redondeamos a 2 decimales para evitar problemas de precisión flotante en los cálculos financieros posteriores
        df['price_sale'] = df['price_sale'].round(2)
        df['unit_cost'] = df['unit_cost'].round(2)

    except Exception as e:
        return None, [f"Error al agrupar los datos diarios: {str(e)}"]

    # Retornamos el DataFrame limpio y agrupado
    # Prophet Ready: Prophet exige que el DataFrame tenga una fecha(ds) y un valor numerico agregado (y). 
    # Con este agrupamiento, el dato ya esta pre-digerido para la Inteligencia Artificial, que se enfocará en predecir la cantidad vendida (qty_sold) a futuro, a nivel diario, por SKU y tienda.
    return df, []


def _validate_catalog(df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], List[str]]:
    missing_cols = [col for col in REQUIRED_CATALOG_COLS if col not in df.columns]
    if missing_cols:
        return None, [f"Faltan columnas obligatorias para Catálogo: {', '.join(missing_cols)}"]

    # Nos quedamos con las requeridas + cualquier opcional que haya hecho match
    cols_to_keep = [c for c in df.columns if c in REQUIRED_CATALOG_COLS or c in ['department', 'category', 'subcategory', 'product_type', 'brand', 'size', 'supplier']]
    df = df[cols_to_keep].copy()

    df['sku_code'] = df['sku_code'].astype(str).str.strip()
    for col in ['price', 'cost']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
    if df[['price', 'cost']].isnull().any().any():
        return None, ["Existen precios o costos inválidos en el catálogo."]

    # Si el cliente repite un SKU en el excel, nos quedamos con la última fila
    df = df.drop_duplicates(subset=['sku_code'], keep='last')
    
    return df, []


def _validate_inventory(df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], List[str]]:
    missing_cols = [col for col in REQUIRED_INVENTORY_COLS if col not in df.columns]
    if missing_cols:
        return None, [f"Faltan columnas obligatorias para Inventario: {', '.join(missing_cols)}"]
    
    # Definimos las columnas opcionales que nos importan.
    optional_cols = ['on_order','safety_stock','lead_time_days']

    cols_to_keep = [c for c in df.columns if c in REQUIRED_INVENTORY_COLS or c in optional_cols]
    df = df[cols_to_keep].copy()

    df['sku_code'] = df['sku_code'].astype(str).str.strip()
    df['store'] = df['store'].astype(str).str.strip()
    df['inv_qty'] = pd.to_numeric(df['inv_qty'], errors='coerce')

    if df['inv_qty'].isnull().any():
        return None, ["Existen cantidades de inventario inválidas, con texto o vacias. Revise su archivo."]
    
    # Construimos un dicconario de agregacion dinamico
    agg_dict = {'inv_qty': 'sum'}  # Siempre sumamos la cantidad de inventario si hay duplicados de SKU/tienda
    for col in optional_cols:
        if col in df.columns:
            # Forzamos a numero (si vienen vacion o con texto, los volvemos 0 para no romper la DB)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            # Para las columnas opcionales, (ajustar esto según la lógica de negocio.)
            # Si la columna es 'on_order', tiene sentido promediarla si hay duplicados, porque podría representar una cantidad en tránsito que se reparte entre varias filas.
            # Y para no sobreetimar cantidades que no sabemos si se van a recibir, es mejor promediarla. En cambio, para 'safety_stock' o 'lead_time_days',
            # tiene más sentido tomar el máximo, porque queremos asegurarnos de tener suficiente stock de seguridad o el tiempo de entrega más largo en caso de discrepancias.
            if col in df.columns == 'on_order':  
                agg_dict[col] = 'mean'

            agg_dict[col] = 'max'  

    # Agrupamos aplicando nuestro diccionario dinamico.
    try:
        df = df.groupby(['sku_code', 'store'], as_index=False).agg(agg_dict)
    except Exception as e:
        return None, [f"Error al consolidar los datos de inventario, revise sus columnas: {str(e)}"]

    return df, []
    

