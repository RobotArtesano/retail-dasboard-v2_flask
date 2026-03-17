from flask import Blueprint

# 1. CREAMOS EL BLUEPRINT
bp = Blueprint('auth', __name__)

# 2. IMPORTAMOS LAS RUTAS DESPUES DE CREAR EL BLUEPRINT PARA EVITAR IMPORTACIONES CIRCULARES
from app.auth import routes