from flask import redirect, render_template, session
from functools import wraps

def apology(message, code=400):
    """Render message as an apology to user."""

    def escape(s):
        """
        Escape special characters.

        https://github.com/jacebrowning/memegen#special-characters
        """
        for old, new in [
            ("-", "--"),
            (" ", "-"),
            ("_", "__"),
            ("?", "~q"),
            ("%", "~p"),
            ("#", "~h"),
            ("/", "~s"),
            ('"', "''"),
        ]:
            s = s.replace(old, new)
        return s

    return render_template("apology.html", top=code, bottom=escape(message)), code


def login_required(f):
    """
    Decorate routes to require login.
    Decorador personalizado para proteger rutas en Flask

    https://flask.palletsprojects.com/en/latest/patterns/viewdecorators/
    """

    # Es un decorador que preserva la metadata de funciones cuando creas tus propios decoradores.
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if session.get("user_id") is None:
            return redirect("/login")
        return f(*args, **kwargs)

    return decorated_function


# PRIMER INTENTO DE FUNCIONES PARA INIDICADORES

from app import db
from app.models import Sale, Product, Inventory
from sqlalchemy import func

def get_financial_summary(user_id):
    """
    Calcula las ventas totales, el costo y el margen bruto del usuario.
    """

    # Usamos func.sum para que la bse de datos haga la multiplicacion y suma en lugar de traer todos los registros a memoria
    summary = db.session.query(
        func.sum(Sale.qty_sold * Sale.price_sale).label('total_sales'),
        func.sum(Sale.qty_sold * Sale.unit_cost).label('total_cost')
    ).filter(Sale.user_id == user_id).first()

    # si no hay ventas, result.total_sales sera None, asi que lo convertimos a 0 para evitar problemas al calcular el margen
    total_sales = summary.total_sales or 0.0
    total_cost = summary.total_cost or 0.0

    profit = total_sales - total_cost
    # Evitamos la division por cero, si total_sales es 0, el margen porcentual se considera 0%
    margin_percentage = (profit / total_sales * 100) if total_sales > 0 else 0.0

    return {
        "total_sales": round(total_sales, 2),
        "total_cost": round(total_cost, 2),
        "profit": round(profit, 2),
        "margin_percentage": round(margin_percentage, 2)
    }

def get_inventory_valuation(user_id):
    """
    Calcula cuanto dinero tiene el usuario "congelado" en el inventario.
    Aqui cruzamos (JOIN) la tabla de inventario con la tabla de productos para obtener el costo unitario de cada producto y multiplicarlo por la cantidad en inventario.
    """

    # Hacemos un join entre Inventory y Product para obtener el costo unitario de cada producto
    result = db.session.query(
        func.sum(Inventory.inv_qty * Product.cost).label('total_valuation')
    ).join(
        # Unimos las tablas usando el SKU y asegurandonos que sea el mismo usuario
        Product, (Inventory.product_id == Product.id) & (Inventory.user_id == Product.user_id)
    ).filter(Inventory.user_id == user_id).first()

    total_valuation = result.total_valuation or 0.0

    return round(total_valuation, 2)