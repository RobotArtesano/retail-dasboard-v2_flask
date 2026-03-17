# La carpeta utils NO es un un Blueprint. NO manneja rutas de navegacion, ni vistas, ni templates. Es solo un conjunto de funciones auxiliares 
# que pueden ser usadas por cualquier módulo (auth, analytics, dashboard) para tareas comunes como procesamiento de datos, validaciones, etc.

# este __init__.py puede estar vacío, su funcion es decirle al interprete de Python que esta carpeta es un paquete, 
# y permitir la importacion de funciones desde otros modulos dentro de utils.

# EXISTE EL PATRON FACADE (para importaciones limpias): pero requiere mantenimiento constante a medida que se agregan nuevas funciones. Por ahora, 
# importaremos directamente las funciones necesarias en cada módulo (dashboard, analytics, etc.) para evitar un mantenimiento extra.

"""
Ejemplo de cómo se vería un patrón Facade (no implementado, solo como referencia):
# app/utils/__init__.py

# Importamos las funciones clave de los submódulos

from .supplychain import process_upload, normalize_columns

# Cuando creemos las funciones financieras, las agregaremos aquí:
# from .finance import calculate_roi, calculate_margin

# Definimos explícitamente qué funciones están disponibles para el resto de la App

__all__ = ['process_upload', 'normalize_columns']

"""