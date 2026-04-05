import os
from dotenv import load_dotenv 

# NUNCA SUBIR A GITHUB EL ARCHIVO .env, YA QUE CONTIENE INFORMACIÓN SENSIBLE COMO CLAVES SECRETAS O URL DE BASES DE DATOS.
# Para evitar subir el archivo .env a GitHub, se puede agregar a un archivo .gitignore la línea:
# .env

# Esto carga las variables del archivo .env a la memoria del sistema
load_dotenv()

# __file__ obtiene la ruta de config.py, y dirname obtiene la carpeta donde esta. Esto es útil para construir rutas relativas a la ubicación del archivo config.py, como la ruta de la base de datos SQLite.
basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    # --- SEGURIDAD CORE ---
    # os.environ.get busca la variable de entorno, si no la encuentra, usa segundo valor como respaldo (fallback). En producción, se debe establecer la variable de entorno SECRET_KEY con un valor seguro y aleatorio.
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret')

    # --- Seguridad de Acceso (codigo por invitacion)---
    REGISTRATION_CODE = os.environ.get('REGISTRATION_CODE')  # Codigo de invitacion para registro. Se valida en auth/routes.py, si no coincide, se rechaza el registro.

    # --- CONFIGURACIÓN DE BASE DE DATOS ---
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or 'sqlite:///' + os.path.join(basedir, 'app.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Escudos de seguridad para cookies, recomendados para proteger contra ataques comunes como XSS o CSRF.
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'  # O 'Strict' para mayor seguridad, pero puede afectar la funcionalidad en algunos casos.
    SESSION_COOKIE_SECURE = False  # Asegura que las cookies solo se envíen a través de HTTPS (obligatorio en producción), en desarrollo local se puede dejar en False.

    # Cloudflare R2 Configuration
    R2_BUCKET = os.environ.get('R2_BUCKET_NAME')
    R2_ACCESS_KEY = os.environ.get('R2_ACCESS_KEY')
    R2_SECRET_KEY = os.environ.get('R2_SECRET_KEY')
    R2_ENDPOINT = os.environ.get('R2_ENDPOINT_URL')

    # Limite maximo de tamaño de archivo subido (16MB)
    # suficiente para unas 200,000 filas de CSV)
    # Si se supera este límite, Flask lanzará un error 413 Request Entity Too Large.
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024