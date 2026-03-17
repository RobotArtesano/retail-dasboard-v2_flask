from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from app import db
from app.models import Sale, Product, Inventory
from app.utils.supplychain import process_upload
import numpy as np # para manejar valores nulos de pandas y convertirlos a None para la base de datos

bp = Blueprint('dashboard', __name__)

@bp.route('/', methods=['GET'])
@login_required
def index():
    """Renderiza la vista principal del Dasboard con logica de Onboarding."""

    # Hacemos una consulta rápida para saber si el usuario tiene datos de catalogo cargados, 
    # esto nos sirve para mostrar un mensaje de bienvenida personalizado y guiarlo a cargar su primer archivo.
    primer_producto = Product.query.filter_by(user_id=current_user.user_id).first()
    if not primer_producto:
        flash("¡Bienvenido al Dashboard! Parece que aún no has cargado tu catálogo de productos. Para empezar, sube un archivo con tus productos (SKU, nombre, costo, precio, etc.) usando el formulario de carga. Esto nos ayudará a mantener la integridad referencial cuando cargues tus ventas e inventarios. ¡Vamos a comenzar!", "info")

    # Si primer_producto tiene datos, has_catalog sera True. Si es None, has_catalog sera False.
    has_catalog = primer_producto is not None

    # Pasamos esta variable booleana a nuestra plantilla HTML
    return render_template('dashboard/index.html', has_catalog=has_catalog)


@bp.route('/upload', methods=['POST'])
@login_required
def upload_file():
    """Recibe el archivo, invoca el ETL y guarda en la base de datos de forma masiva."""

    # 1. Validacion inicial del archivo en el Request
    if 'file' not in request.files:
        flash("No se encontro ningun archivo en la peticion.", "error")
        return redirect(url_for('dashboard.index'))
    
    file = request.files['file']
    if file.filename == '':
        flash("No se ha seleccionado ningún archivo.", "error")
        return redirect(url_for('dashboard.index'))
    
    upload_type = request.form.get('upload_type')
    if upload_type not in ['sales', 'catalog', 'inventory']:
        flash("Tipo de carga no especificado o inválido. Por favor seleccione Ventas, Catalogo o Inventario.", "error")
        return redirect(url_for('dashboard.index'))
    
    # Leemos el archivo en memoria como bytes para pasarlo a la función de procesamiento
    file_bytes = file.read()

    # 2. Invocamos nuestro Motor Multi-ETL (pandas) para procesar los archivos, normalizarlo y validar su contenido
    df, errores = process_upload(file_bytes, file.filename, upload_type)

    if errores:
        # si la función de procesamiento retorna errores, los mostramos al usuario y no intentamos guardar nada en la base de datos.
        for error in errores:
            flash(error, "error")
        return redirect(url_for('dashboard.index'))
    
    # Para evitar errores con NaNs de Pandas al pasar a la base de datos, convertimos cualquier valor NaN a None 
    # (que es lo que SQLAlchemy espera para campos nulos, Null de SQL)
    df = df.replace({np.nan: None})
    
    try:
        # ==========================================
        # RUTA 1: CARGA DE VENTAS (Agregar Histórico)
        # ==========================================
        if upload_type == 'sales':
            skus_en_archivo = df['sku_code'].unique().tolist()

            # Upsert de SKUs faltantes (silencioso, sin mostrar errores al usuario, solo para mantener la integridad referencial)
            _asegurar_catalogo(skus_en_archivo)

            # Mapeo de texto (SKU) a entero (product_id)
            mapa_skus = _obtener_mapa_skus(skus_en_archivo)
            df['product_id'] = df['sku_code'].map(mapa_skus)
            df['user_id'] = current_user.user_id
            df.drop(columns=['sku_code'], inplace=True)

            registros = df.to_dict(orient='records')
            db.session.bulk_insert_mappings(Sale, registros)
            db.session.commit()
            flash(f"Se guardaron {len(df)} registros de ventas agregadas (por dia).", "success")
        
       # ==========================================
        # RUTA 2: CARGA DE CATÁLOGO (Upsert Puro)
        # ==========================================
        # Obtenemos los SKUs unicos que vienen en el archivo que el usuario acaba de subir
        elif upload_type == 'catalog':
            skus_en_archivo = df['sku_code'].unique().tolist()

            # Buscamos que SKUs de esa lista Ya existen en la base de datos para este usuario
            existentes = Product.query.filter(
                Product.sku_code.in_(skus_en_archivo),
                Product.user_id == current_user.user_id
            ).all()

            dict_existentes = {p.sku_code: p for p in existentes}
            nuevos_productos = []

            # Recorremos el DataFrame validado
            # orient='records' convierte cada fila del DataFrame en un diccionario, con las columnas como claves y los valores de esa fila como valores.
            # .to_dict(orient) tiene varias opciones como 'dict', 'list', 'series', 'split', 'records'. En este caso, 'records' es ideal para iterar sobre filas como diccionarios.
            for row in df.to_dict(orient='records'):
                sku = row['sku_code']
                if sku in dict_existentes:
                    # UPDATE: Si el SKU ya existe, actualizamos sus campos (excepto el user_id y sku_code que son inmutables)
                    prod = dict_existentes[sku]
                    prod.name = row['name']
                    prod.cost = row['cost']
                    prod.price = row['price']
                    # Opcionales
                    prod.department = row.get('department', prod.department)
                    prod.category = row.get('category', prod.category)
                    prod.subcategory = row.get('subcategory', prod.subcategory)
                    prod.product_type = row.get('product_type', prod.product_type)
                    prod.brand = row.get('brand', prod.brand)
                    prod.size = row.get('size', prod.size)
                    prod.supplier = row.get('supplier', prod.supplier)

                else:
                    # INSERT: Si el SKU no existe, lo agregamos a la lista de nuevos productos para insertar masivamente después del loop
                    nuevos_productos.append(
                        Product(
                            user_id=current_user.user_id,
                            sku_code=sku,
                            name=row['name'],
                            cost=row['cost'],
                            price=row['price'],
                            department=row.get('department'),
                            category=row.get('category'),
                            subcategory=row.get('subcategory'),
                            product_type=row.get('product_type'),
                            brand=row.get('brand'),
                            size=row.get('size'),
                            supplier=row.get('supplier')
                        )
                    )

            if nuevos_productos:
                db.session.bulk_save_objects(nuevos_productos)
            db.session.commit()
            flash(f"Catalogo procesado: {len(nuevos_productos)} nuevos productos creados, {len(existentes)} productos actualizados.", "success")

        # ==========================================
        # RUTA 3: CARGA DE INVENTARIO (Upsert Puro)
        # ==========================================
        elif upload_type == 'inventory':
            skus_en_archivo = df['sku_code'].unique().tolist()
            _asegurar_catalogo(skus_en_archivo) # Nos aseguramos de que existan en el catálogo
            
            mapa_skus = _obtener_mapa_skus(skus_en_archivo)

            # Reemplazamos sku_code por product_id para buscar en BD
            df['product_id'] = df['sku_code'].map(mapa_skus)

            existentes = Inventory.query.filter(
                Inventory.user_id == current_user.user_id,
                Inventory.product_id.in_(list(mapa_skus.values()))
            ).all()

            # La llave para buscar un inventario es una tupla: (product_id, store)
            dict_existentes = {(inv.product_id, inv.store): inv for inv in existentes}
            nuevos_inventarios = []

            for row in df.to_dict(orient='records'):
                p_id = row['product_id']
                tienda = row['store']
                llave = (p_id, tienda)

                if llave in dict_existentes:
                    # UPDATE: Actualizamos el inventario existente con la nueva cantidad y fecha de actualización
                    inv = dict_existentes[llave]
                    inv.qty_qty = row['inv_qty']
                    inv.on_order = row.get('on_order', inv.on_order)
                    inv.safety_stock = row.get('safety_stock', inv.safety_stock)
                    inv.lead_time_days = row.get('lead_time_days', inv.lead_time_days)
                else:
                    # INSERT: Si no existe, lo agregamos a la lista de nuevos inventarios para insertar masivamente después del loop
                    nuevos_inventarios.append(
                        Inventory(
                            user_id=current_user.user_id,
                            product_id=p_id,
                            store=tienda,
                            inv_qty=row['inv_qty'],
                            on_order=row.get('on_order', 0),
                            safety_stock=row.get('safety_stock', 0),
                            lead_time_days=row.get('lead_time_days', 0)
                        )
                    )

            if nuevos_inventarios:
                db.session.bulk_save_objects(nuevos_inventarios)
            db.session.commit()

            flash(f"Inventario procesado: {len(nuevos_inventarios)} nuevos registros creados, {len(existentes)} registros actualizados. Combinaciones (SKU/Tienda)", "success")

        return redirect(url_for('dashboard.index'))

    except Exception as e:
        # En caso de cualquier error inesperado durante el proceso, hacemos rollback de la transacción para evitar datos corruptos
        db.session.rollback()
        flash(f"Ocurrió un error al guardar en la base de datos: {str(e)}", "error")
        return redirect(url_for('dashboard.index'))
    


# ==== FUNCIONES DE AYUDA Privadas(Helpers Internos) ====
def _asegurar_catalogo(lista_skus):
    """Verifica si los SKUs existen, si no, los crea en blanco para mantener la integridad referencial."""
    existentes = Product.query.filter(
        Product.sku_code.in_(lista_skus),
        Product.user_id == current_user.user_id
    ).all()
    skus_existentes = {p.sku_code for p in existentes}
    skus_nuevos = set(lista_skus) - skus_existentes

    if skus_nuevos:
        nuevos_productos = [
            Product(
                user_id=current_user.user_id,
                sku_code=sku,
                name="Producto Nuevo (Auto-generado)",
                cost=0.0,
                price=0.0
            ) for sku in skus_nuevos
        ]
        db.session.bulk_save_objects(nuevos_productos)
        db.session.commit()

def _obtener_mapa_skus(lista_skus):
    """Obtiene un diccionario de mapeo de sku_code a product_id para el usuario actual.
        ejemplo: {"SKU123": 1 (id_interno), "SKU456": 2 (id_interno), ...}"""
    productos = Product.query.filter(
        Product.sku_code.in_(lista_skus),
        Product.user_id == current_user.user_id
    ).all()
    return {p.sku_code: p.id for p in productos}