from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from config import Config
from flask_wtf.csrf import CSRFProtect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from redis import Redis
import rq

# Inicialización de la aplicación Flask, la base de datos SQLAlchemy, el LoginManager, conexion a Redis y el Limiter para limitar la cantidad de solicitudes a la aplicación.
# Este archivo convierte la carpeta app en un paquete de Python, permitiendo la importación de módulos dentro de la carpeta.

db = SQLAlchemy()
login_manager = LoginManager()
csrf = CSRFProtect()
limiter = Limiter(key_func=get_remote_address, default_limits=["200 per day", "50 per hour"])

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    # Inicializar extensiones
    db.init_app(app)
    login_manager.init_app(app)
    csrf.init_app(app)
    limiter.init_app(app)

    # Conexión a Redis para RQ (Redis Queue) - para tareas en segundo plano como el pronóstico de ventas con Prophet.
    # Por defecto, Redis corre en el puerto 6379 localmente, y usamos la base de datos 0 para esta aplicación. 
    app.redis = Redis.from_url('redis://localhost:6379/0')
    # Creamos una cola llamada 'forecast-tasks'
    app.task_queue = rq.Queue('forecast-tasks', connection=app.redis)

    

    # Configuracion de Flask-Login 
    login_manager.login_view = 'auth.login'  # Redirige a la página de login si el usuario no está autenticado
    login_manager.login_message = 'Por favor, inicie sesión para acceder a esta página.'
    login_manager.login_message_category = 'error'

    # IMPORTAR Y REGISTRAR BLUEPRINTS
    from app.auth.routes import bp as auth_bp
    app.register_blueprint(auth_bp, url_prefix='/auth') # las rutas de auth estarán bajo el prefijo /auth (ej: /auth/login, /auth/register)

    # from app.analytics.routes import bp as analytics_bp
    # app.register_blueprint(analytics_bp, url_prefix='/analytics')

    from app.dashboard.routes import bp as dashboard_bp
    # Al dashboard es la página principal, no le asignamos un prefijo de URL, para que esté disponible en la raíz del sitio.
    app.register_blueprint(dashboard_bp)

    # Utils lo importaremos directamente en los módulos que lo necesiten, no es un blueprint, es solo un conjunto de funciones auxiliares.
    
    # =--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # DEBUG: Imprimir las rutas disponibles en la aplicación para verificar que los blueprints se han registrado correctamente. Esto se muestra al correr flask, no es necesario descomentar para verlas, ya que Flask las muestra por defecto al iniciar la aplicación.    
    # Linea para debuggear las rutas disponibles en la app se muestran al correr flask(descomentar para verlas en consola)
    print(app.url_map)

    return app