import os
from flask import Blueprint, render_template, redirect, url_for, flash, request
from flask_login import login_user, logout_user, login_required, current_user
from app import db, limiter
from app.models import User

# Definimos el Blueprint para este módulo
bp = Blueprint('auth', __name__)

@bp.route('/register', methods=['GET', 'POST'])
@limiter.limit("5 per hour")  # Limitar a 5 registros por hora por IP para prevenir abuso
def register():
    """Registra un nuevo usuario utilizando SQLAlchemy."""
    # Si el usuario ya está loggeado, no tiene sentido que vea esta página
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.index'))

    if request.method == 'POST':

        # ACCESO CON CODIGO DE INVITACION: Para controlar quién puede registrarse, agregamos un campo de código de invitación en el formulario de registro.
        # 1. Recibimos el código que escribió el usuario y el código real del .env
        codigo_ingresado = request.form.get('access_code')
        codigo_secreto = os.environ.get('REGISTRATION_CODE')

        # 2. El Guardia de Seguridad: Si no coinciden, lo rebotamos inmediatamente
        if codigo_ingresado != codigo_secreto:
            flash('Código de invitación inválido. Acceso denegado.', 'error')
            return redirect(url_for('auth.register'))
        # =========================================================================

        # 3. Si el código es correcto, continuamos con tu lógica normal de registro
        username = request.form.get("username")
        email = request.form.get("email")
        password = request.form.get("password")
        confirmation = request.form.get("confirmation")

        # 1. Validaciones básicas de entrada
        if not username or not email or not password:
            flash("Todos los campos son obligatorios.", "error")
            return redirect(url_for('auth.register'))
        
        if password != confirmation:
            flash("Las contraseñas no coinciden.", "error")
            return redirect(url_for('auth.register'))

        # 2. Verificar si el usuario o email ya existen (ORM en lugar de SQL crudo)
        user_exists = User.query.filter_by(username=username).first()
        email_exists = User.query.filter_by(email=email).first()
        
        if user_exists or email_exists:
            flash("El nombre de usuario o correo electrónico ya está en uso.", "error")
            return redirect(url_for('auth.register'))

        # 3. Crear el objeto Usuario y guardarlo en la Base de Datos
        new_user = User(username=username, email=email)
        new_user.set_password(password) # Usamos el método seguro que creamos en models.py
        
        db.session.add(new_user)
        db.session.commit()

        flash("¡Registro exitoso! Por favor, inicia sesión.", "success")
        return redirect(url_for('auth.login'))

    # Método GET: Mostrar el formulario
    return render_template("auth/register.html")


@bp.route('/login', methods=['GET', 'POST'])
@limiter.limit("10 per hour")  # Limitar a 10 intentos de login por hora por IP para prevenir ataques de fuerza bruta
def login():
    """Inicia sesión utilizando Flask-Login."""
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.index'))

    if request.method == 'POST':
        username = request.form.get("username")
        password = request.form.get("password")

        if not username or not password:
            flash("Debes ingresar usuario y contraseña.", "error")
            return redirect(url_for('auth.login'))

        # Buscar al usuario
        user = User.query.filter_by(username=username).first()

        # Validar existencia y contraseña
        # No revela si fallo usuario o contraseña para mayor seguridad
        if user is None or not user.check_password(password):
            flash("Usuario o contraseña inválidos.", "error")
            return redirect(url_for('auth.login'))

        # FLASK-LOGIN MÁGICO: Esto reemplaza a tu antiguo session["user_id"] = user.id
        # Crea cookies de sesión seguras y persistentes (remember=True) y maneja todo el proceso de autenticación por ti.
        login_user(user, remember=True)
        
        flash(f"Bienvenido de nuevo, {user.username}.", "success")
        
        # Redirección inteligente: Si intentó ir a una ruta protegida antes de loggearse, lo mandamos ahí
        # Evitamos redirecciones abiertas asegurándonos que el next_page sea una ruta interna válida
        # Si no hay next_page o es una URL externa, lo mandamos al dashboard por defecto. 
        # ej: ?next=http://malicious.com/steal-cookies sería bloqueado porque no empieza con '/'
        next_page = request.args.get('next')
        if not next_page or not next_page.startswith('/'):
            next_page = url_for('dashboard.index')
            
        return redirect(next_page)

    return render_template("auth/login.html")


@bp.route('/logout')
@login_required
def logout():
    """Cierra la sesión del usuario."""
    logout_user() # Limpia las cookies de sesión de forma segura
    flash("Has cerrado sesión exitosamente.", "success")
    return redirect(url_for('auth.login'))


@bp.route('/changepassword', methods=['GET', 'POST'])
@login_required
def change_password():
    """Permite al usuario cambiar su contraseña."""
    if request.method == 'POST':
        current_password = request.form.get("current_password")
        new_password = request.form.get("new_password")
        confirmation = request.form.get("confirmation")

        if not current_password or not new_password or not confirmation:
            flash("Todos los campos son obligatorios.", "error")
            return redirect(url_for('auth.change_password'))

        # Validar la contraseña actual usando el objeto current_user proveído por Flask-Login
        if not current_user.check_password(current_password):
            flash("La contraseña actual es incorrecta.", "error")
            return redirect(url_for('auth.change_password'))

        if new_password != confirmation:
            flash("Las nuevas contraseñas no coinciden.", "error")
            return redirect(url_for('auth.change_password'))

        # Actualizar contraseña
        current_user.set_password(new_password)
        db.session.commit() # Guardamos los cambios

        flash("Contraseña actualizada exitosamente.", "success")
        return redirect(url_for('dashboard.index'))

    return render_template("auth/changepassword.html")