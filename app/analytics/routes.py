from flask import Blueprint, render_template, request, jsonify, current_app
from flask_login import login_required, current_user
from rq.job import Job
from rq.exceptions import NoSuchJobError
from app import db
from app.models import ForecastResult, Product, Sale
from sqlalchemy import func

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
    data = request.get_json()
    granularidad = data.get('granularidad')

    if granularidad not in ('global', 'sku_store'):
        return jsonify({'error': 'Granularidad inválida'}), 400

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
        args=(current_user.user_id, granularidad),
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
        status   = job.get_status()          # queued (esperando) | started | finished | failed
        meta     = job.meta or {}            # progreso y mensajes que la tarea escribe, meta es un diccionario que yo guardo en el worker
        result   = job.result if status == 'finished' else None

        return jsonify({
            'status'  : str(status),
            'progress': meta.get('progress', 0),      # 0–100
            'message' : meta.get('message', ''),
            'result'  : result
        })

    except NoSuchJobError:
        return jsonify({'status': 'not_found, Invalid Job ID or expired'}), 404