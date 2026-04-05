from datetime import datetime, timezone
from flask_login import UserMixin
from werkzeug.security import check_password_hash, generate_password_hash
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.sql import func
from app import db, login_manager

# MODELO MULTI-TENANT: Cada usuario tiene su propio conjunto de datos, con relaciones entre tablas para mantener la integridad referencial.
# Preparado para analisis de datos y generación de pronósticos, con validación de tipos básicos a nivel esquema para asegurar la calidad de los datos.

class User(UserMixin, db.Model):  
    # hereda de db.Model: convierte esta clase en un modelo de SQLAlchemy, y de 
    # UserMixin: agrega métodos útiles para la gestión de usuarios en Flask-Login: is_authenticated, is_active, is_anonymous y get_id.
    __tablename__ = 'users'

    # index=True: Crea un índice en esta columna para acelerar las consultas de búsqueda.
    user_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(db.String(64), index=True, unique=True, nullable=False)
    email = db.Column(db.String(120), index=True, unique=True, nullable=False) # permite login por email
    is_verified = db.Column(db.Boolean, default=False)  # Para futuras funcionalidades de verificación de email

    # Linea para el futuro: (Super admin, permisos, roles, etc)

    is_admin = db.Column(db.Boolean, default=False, nullable=False)  # Para futuras funcionalidades de administración y gestión de usuarios

    # =========================================================
    password_hash = db.Column(db.String(200), nullable=False)
    created_at = db.Column(db.TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    # Relacion con sus datos de ventas
    sales = db.relationship('Sale', backref='owner', lazy='dynamic')
    # Relacion con sus productos
    products = db.relationship('Product', backref='owner', lazy='dynamic')
    # Relacion con sus inventarios
    inventory_records = db.relationship('Inventory', backref='owner', lazy='dynamic')
    # Relacion con sus pronósticos
    forecasts = db.relationship('ForecastResult', backref='owner', lazy='dynamic')

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    
    # ARREGLO PARA FLASK-LOGIN: por defecto, Flask-Login espera que el modelo de usuario tenga un campo 'id' como llave primaria. 
    # Si usas otro nombre (como user_id), debes indicarle a Flask-Login cómo cargar el usuario desde la sesión.
    def get_id(self):
        # """Sobrescribe el método get_id para que Flask-Login use user_id en lugar de id."""
        return str(self.user_id)
    # =======================================================================================
    

class Product(db.Model):
    __tablename__ = 'catalog'

    # Usamos un ID interno unico para la DB (llave primaria), y el SKU se almacena como un campo separado. 
    # Esto permite que el usuario pueda subir nuevos catálogos con SKUs diferentes sin afectar la integridad referencial de las ventas históricas.
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.user_id'), nullable=False)

    # SKU que ve el usuario, usamos string por seguridad, ya que algunos SKU pueden contener letras o caracteres especiales.
    sku_code = db.Column(db.String(50), nullable=False, index=True)

    name = db.Column(db.String(100), nullable=False)
    department = db.Column(db.String(50))
    category = db.Column(db.String(50))
    subcategory = db.Column(db.String(50))
    product_type = db.Column(db.String(50))
    brand = db.Column(db.String(50))
    size = db.Column(db.String(20))
    cost = db.Column(db.Float, nullable=False)
    price = db.Column(db.Float, nullable=False)
    supplier = db.Column(db.String(100))
    created = db.Column(db.TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    
    # Unique constraint para evitar registros duplicados de ventas (mismo usuario, fecha, SKU y tienda)
    # Las bases de datos crean indices únicos automáticamente para estas columnas, lo que mejora el rendimiento de las consultas y
    #  garantiza la integridad de los datos.
    # El SKU debe ser unico por cada user_id (Unique Constraint)
    __table_args__ = (
        db.UniqueConstraint('sku_code', 'user_id', name='uq_sku_per_user'),
    )

    # --- Relación (Opcional pero muy recomendado) ---
    # Esto te permite acceder a los registros de inventario asociados a un producto
    inventory_records = db.relationship('Inventory', back_populates='product')


class Sale(db.Model):
    """
    Docstring for Sale

    Almacena los datos historicos de ventas, con una relacion de muchos a uno con el usuario 
    Incluye validacion de tipos basicos a nivel esquema.
    """
    __tablename__ = 'sales'

    sale_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.user_id'), nullable=False)

    # Vinculamos al ID interno del producto en el catálogo, no al SKU directamente, 
    # para mantener la integridad referencial y evitar problemas si el usuario sube un nuevo catálogo con SKUs diferentes.
    product_id = db.Column(db.Integer, db.ForeignKey('catalog.id'), nullable=False)
    
    date = db.Column(db.Date, nullable=False, index=True)
    store = db.Column(db.String(100), nullable=False, index=True)
    qty_sold = db.Column(db.Integer, nullable=False)

    # Snapshots
    price_sale = db.Column(db.Float, nullable=False)
    unit_cost = db.Column(db.Float, nullable=False)

    __table_args__ = (
        db.UniqueConstraint('user_id', 'product_id','date', 'store', 
                            name='uq_sale_day_store_product'),)

    def ingreso_bruto(self):
        return self.qty_sold * self.price_sale
    
    def costo_total(self):
        return self.qty_sold * self.unit_cost
    
    # no se guarda en la base de datos, se calcula al vuelo cada vez que se accede a esta propiedad
    @property
    def margin(self):
        """Calculo dinammico del margen de ganancia para cada venta"""
        return (self.price_sale - self.unit_cost) * self.qty_sold


class Inventory(db.Model):
    __tablename__ = 'inventory'

    user_id = db.Column(db.Integer, db.ForeignKey('users.user_id'), nullable=False)

    # Vinculamos al ID interno del producto en el catálogo, no al SKU directamente
    product_id = db.Column(db.Integer, db.ForeignKey('catalog.id'), nullable=False)


    store = db.Column(db.String(100), nullable=False, index=True)
    inv_qty = db.Column(db.Integer, default=0, nullable=False)
    on_order = db.Column(db.Integer, default=0, nullable=False)
    safety_stock = db.Column(db.Integer, default=0, nullable=False)
    lead_time_days = db.Column(db.Integer, nullable=False)

     # --- Argumentos y Restricricciones de la Tabla ---
    # Aquí se define la llave primaria compuesta por 'product_id' y 'store'
    __table_args__ = (
        PrimaryKeyConstraint('product_id', 'store', 'user_id'),
    )

    # --- Relación (Opcional pero muy recomendado) ---
    # Esto te permite acceder al objeto 'Catalog' desde un objeto 'Inventory'
    # Por ejemplo: mi_item_de_inventario.product.name
    product = db.relationship('Product', back_populates='inventory_records')

    @property
    def below_safety_stock(self):
        return self.inv_qty < self.safety_stock


class ForecastResult(db.Model):
    __tablename__ = 'forecasts'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.user_id'), nullable=False)

    # Vinculamos al ID interno del producto en el catálogo, no al SKU directamente
    product_id = db.Column(db.Integer, db.ForeignKey('catalog.id'), nullable=False)
    store = db.Column(db.String(100), nullable=False, index=True)

    date = db.Column(db.Date, nullable=False)
    yhat = db.Column(db.Float)  # Pronóstico central
    yhat_lower = db.Column(db.Float)  # Límite inferior del pronóstico
    yhat_upper = db.Column(db.Float)  # Límite superior del pronóstico

    # Unique constraint para evitar tener dos pronósticos para el mismo producto el mismo día-tienda para el mismo usuario.
    __table_args__ = (
        db.UniqueConstraint('product_id', 'store', 'date', 'user_id', name='uq_forecast_store_date'),
    )

# Obligatorio para que LoginManager pueda cargar el usuario desde la sesión, 
# convierte el id guardado en cookies en un objeto User real de la base de datos.
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))