from flask import Blueprint, render_template, request, jsonify, current_app
from flask_login import login_required, current_user
from rq.job import Job
from rq.exceptions import NoSuchJobError
from app import db
from app.models import ForecastResult, Product, Sale, Inventory
from sqlalchemy import func, distinct
import json
import math

bp = Blueprint('analytics', __name__)

@bp.route('/forecast')
@login_required
def forecast():
    """Vista principal. Muestra el selector de granularidad y resultados previos."""

    # ¿El usuario tiene suficientes datos para entrenar?
    total_registros = db.session.query(func.count(Sale.sale_id)).filter(Sale.user_id == current_user.user_id).scalar() or 0

    # Pronósticos previos (para saber si ya entrenó alguna vez)
    tiene_forecast = db.session.query(ForecastResult.id)\
        .filter(ForecastResult.user_id == current_user.user_id)\
        .first() is not None

    return render_template(
        'analytics/forecast.html',
        total_registros=total_registros,
        tiene_forecast=tiene_forecast
    )


@bp.route('/forecast/train', methods=['POST'])
@login_required
def train():
    """
    Recibe la granularidad elegida por el usuario y encola la tarea de entrenamiento.
    Devuelve JSON con el job_id para que el frontend haga polling del estado de la tarea.
    """
    data = request.get_json(silent=True)
    print("DEBUG data recibida:", data)   # ← agrega esta línea para verificar que el frontend envía correctamente la granularidad y el horizonte.

    if not data:
        return jsonify({'error': 'No se recibió JSON válido'}), 400

    granularidad = data.get('granularidad')
    horizonte = int(data.get('horizonte', 30))  # Días a pronosticar, por defecto 30

    print(f"DEBUG granularidad={granularidad} tipo={type(granularidad)}")
    print(f"DEBUG horizonte={horizonte} tipo={type(horizonte)}")

    if granularidad not in ('global', 'sku_store'):
        return jsonify({'error': 'Granularidad inválida'}), 400

    if horizonte not in (30, 60, 90, 120):
        return jsonify({'error': 'Horizonte inválido'}), 400
    
    # Verificamos que el usuario tiene datos antes de encolar la tarea
    tiene_datos = db.session.query(Sale.sale_id)\
        .filter(Sale.user_id == current_user.user_id)\
        .first()

    if not tiene_datos:
        return jsonify({'error': 'No tienes registros de ventas cargados.'}), 400

    # Encolamos la tarea pesada en Redis usando RQ
    # Importamos aqui para evitar problemas de importación circular
    from app.analytics.tasks import tarea_entrenamiento

    job = current_app.task_queue.enqueue(
        tarea_entrenamiento,
        args=(current_user.user_id, granularidad, horizonte),
        job_timeout=3600   # 1 hora máximo (Prophet sobre muchos SKUs puede tardar)
    )

    return jsonify({'job_id': job.id}), 202


@bp.route('/forecast/status/<job_id>')
@login_required
def forecast_status(job_id):
    """
    Endpoint de polling(patron de preguntar cada cierto tiempo el estado). El frontend llama esto cada N segundos
    para saber si el entrenamiento terminó.

    Estados posibles de RQ: queued | started | finished | failed
    """
    try:
        job = Job.fetch(job_id, connection=current_app.task_queue.connection)
        status_raw = job.get_status()        # queued (esperando) | started | finished | failed
        # Normalizar a string simple compatible con frontend
        status_map = {
            'JobStatus.QUEUED'   : 'queued',
            'JobStatus.STARTED'  : 'started', 
            'JobStatus.FINISHED' : 'finished',
            'JobStatus.FAILED'   : 'failed',
            'queued'             : 'queued',
            'started'            : 'started',
            'finished'           : 'finished',
            'failed'             : 'failed',
        }
        status = status_map.get(str(status_raw), str(status_raw))  # fallback a lo que devuelva Redis Q si no está en el map

        meta     = job.meta or {}            # progreso y mensajes que la tarea escribe, meta es un diccionario que yo guardo en el worker

        result   = None

        if status == 'finished':
            try:
                raw = job.result
                # Serializar a JSON y de vuelta para limpiar tipos numpy
                import json
                # json.dumps convierte a tipos nativos de Python (ej. numpy int64 a int), luego json.loads devuelve un dict limpio
                result = json.loads(json.dumps(raw, default=str))
            except Exception as e:
                print(f"DEBUG result serialize error: {e}")
                result = {'ok': True, 'resumen': {'ensemble': 0, 'moving_average': 0}, 'errores': []}

        return jsonify({
            'status'  : status,
            'progress': meta.get('progress', 0),      # 0–100
            'message' : meta.get('message', ''),
            'result'  : result
        })

    except NoSuchJobError:
        return jsonify({'status': 'not_found, Invalid Job ID or expired'}), 404



# ── /forecast/results ────────────────────────────────────────────────────────
@bp.route('/forecast/results')
@login_required
def forecast_results():
    """Vista principal de resultados. Carga filtros disponibles para el usuario."""

    # Departamentos disponibles (solo los que tienen pronóstico)
    departamentos = db.session.query(distinct(Product.department))\
        .join(ForecastResult, ForecastResult.product_id == Product.id)\
        .filter(ForecastResult.user_id == current_user.user_id)\
        .filter(Product.department.isnot(None))\
        .order_by(Product.department)\
        .all()
    departamentos = [d[0] for d in departamentos]

    # Tiendas disponibles
    tiendas = db.session.query(distinct(ForecastResult.store))\
        .filter(ForecastResult.user_id == current_user.user_id)\
        .order_by(ForecastResult.store)\
        .all()
    tiendas = [t[0] for t in tiendas]

    tiene_forecast = db.session.query(ForecastResult.id)\
        .filter(ForecastResult.user_id == current_user.user_id)\
        .first() is not None

    return render_template(
        'analytics/forecast_results.html',
        departamentos = departamentos,
        tiendas       = tiendas,
        tiene_forecast= tiene_forecast
    )


# ── /forecast/results/data ───────────────────────────────────────────────────
@bp.route('/forecast/results/data')
@login_required
def forecast_results_data():
    """
    API JSON para la tabla dinámica con paginación.
    Parámetros query string:
        departamento, categoria, tienda, page (default 1), per_page (default 50)
    """
    departamento = request.args.get('departamento', '')
    categoria    = request.args.get('categoria', '')
    tienda       = request.args.get('tienda', '')
    page         = int(request.args.get('page', 1))
    per_page     = int(request.args.get('per_page', 50))

    # Query base — agrupamos por SKU-tienda sumando el horizonte completo
    query = db.session.query(
        Product.sku_code,
        Product.name,
        Product.department,
        Product.category,
        ForecastResult.store,
        func.sum(ForecastResult.yhat).label('yhat_total'),
        func.avg(ForecastResult.yhat).label('yhat_avg'),
        func.min(ForecastResult.yhat_lower).label('yhat_lower'),
        func.max(ForecastResult.yhat_upper).label('yhat_upper'),
        func.max(ForecastResult.modelo).label('modelo'),
        func.avg(ForecastResult.mae).label('mae'),
        func.avg(ForecastResult.smape).label('smape'),
        func.count(ForecastResult.id).label('dias')
    )\
    .join(Product, ForecastResult.product_id == Product.id)\
    .filter(ForecastResult.user_id == current_user.user_id)

    # Filtros dinámicos
    if departamento:
        query = query.filter(Product.department == departamento)
    if categoria:
        query = query.filter(Product.category == categoria)
    if tienda:
        query = query.filter(ForecastResult.store == tienda)

    query = query.group_by(
        Product.sku_code, Product.name, Product.department,
        Product.category, ForecastResult.store
    ).order_by(Product.department, Product.category, Product.sku_code)

    # Paginación manual
    total  = query.count()
    offset = (page - 1) * per_page
    rows   = query.offset(offset).limit(per_page).all()

    data = [{
        'sku'        : r.sku_code,
        'nombre'     : r.name,
        'departamento': r.department or '—',
        'categoria'  : r.category   or '—',
        'tienda'     : r.store,
        'yhat_total' : round(float(r.yhat_total), 1) if r.yhat_total else 0,
        'yhat_avg'   : round(float(r.yhat_avg),   1) if r.yhat_avg   else 0,
        'yhat_lower' : round(float(r.yhat_lower), 1) if r.yhat_lower else 0,
        'yhat_upper' : round(float(r.yhat_upper), 1) if r.yhat_upper else 0,
        'modelo'     : r.modelo or '—',
        'mae'        : round(float(r.mae),   2) if r.mae   else None,
        'smape'      : round(float(r.smape), 2) if r.smape else None,
        'dias'       : r.dias,
    } for r in rows]

    return jsonify({
        'data'    : data,
        'total'   : total,
        'page'    : page,
        'pages'   : math.ceil(total / per_page) if total else 1,
        'per_page': per_page
    })


# ── /forecast/results/categorias ─────────────────────────────────────────────
@bp.route('/forecast/results/categorias')
@login_required
def forecast_categorias():
    """
    Devuelve las categorías disponibles para un departamento dado.
    Usado para el filtro en cascada departamento → categoría.
    """
    departamento = request.args.get('departamento', '')

    query = db.session.query(distinct(Product.category))\
        .join(ForecastResult, ForecastResult.product_id == Product.id)\
        .filter(ForecastResult.user_id == current_user.user_id)\
        .filter(Product.category.isnot(None))

    if departamento:
        query = query.filter(Product.department == departamento)

    categorias = [c[0] for c in query.order_by(Product.category).all()]
    return jsonify({'categorias': categorias})


# ── /forecast/results/serie ───────────────────────────────────────────────────
@bp.route('/forecast/results/serie')
@login_required
def forecast_serie():
    """
    Devuelve la serie de tiempo completa para un SKU-tienda específico.
    Incluye historial real + pronóstico con bandas de confianza para Plotly.
    """
    sku_code = request.args.get('sku')
    tienda   = request.args.get('tienda')

    if not sku_code or not tienda:
        return jsonify({'error': 'Faltan parámetros sku y tienda'}), 400

    producto = Product.query.filter_by(
        sku_code = sku_code,
        user_id  = current_user.user_id
    ).first()

    if not producto:
        return jsonify({'error': 'SKU no encontrado'}), 404

    # ── Historial real de ventas ──────────────────────────────────────────────
    ventas = db.session.query(
        Sale.date.label('ds'),
        func.sum(Sale.qty_sold).label('y')
    )\
    .filter(Sale.user_id  == current_user.user_id)\
    .filter(Sale.product_id == producto.id)\
    .filter(Sale.store    == tienda)\
    .group_by(Sale.date)\
    .order_by(Sale.date)\
    .all()

    # ── Pronóstico ────────────────────────────────────────────────────────────
    forecast = ForecastResult.query\
        .filter_by(
            user_id    = current_user.user_id,
            product_id = producto.id,
            store      = tienda
        )\
        .order_by(ForecastResult.date)\
        .all()

    # ── Últimos 90 días del historial para no saturar la gráfica ─────────────
    hist_data = [{'ds': str(v.ds), 'y': float(v.y)} for v in ventas[-90:]]

    fc_data = [{
        'ds'         : str(f.date),
        'yhat'       : round(float(f.yhat),       1) if f.yhat       else 0,
        'yhat_lower' : round(float(f.yhat_lower), 1) if f.yhat_lower else 0,
        'yhat_upper' : round(float(f.yhat_upper), 1) if f.yhat_upper else 0,
    } for f in forecast]

    return jsonify({
        'sku'      : sku_code,
        'nombre'   : producto.name,
        'tienda'   : tienda,
        'historial': hist_data,
        'forecast' : fc_data,
        'modelo'   : forecast[0].modelo if forecast else '—',
        'mae'      : round(float(forecast[0].mae),   2) if forecast and forecast[0].mae   else None,
        'smape'    : round(float(forecast[0].smape), 2) if forecast and forecast[0].smape else None,
    })


