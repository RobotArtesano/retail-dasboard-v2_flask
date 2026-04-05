from flask import Blueprint

# 1. CREAMOS EL BLUEPRINT
bp = Blueprint('dashboard', __name__)

# 2. IMPORTAMOS LAS RUTAS DESPUES DE CREAR EL BLUEPRINT PARA EVITAR IMPORTACIONES CIRCULARES
from app.dashboard import routes