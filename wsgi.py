from app import create_app, db

app = create_app()

# Creando comando para terminal de Flask
# Para crear tablas en la base de datos (solo para uso local) con baase en los modelos definidos en models.py
@app.cli.command('init-db')
def init_db():
    """Crea tablas (para uso local)."""
    with app.app_context():
        db.create_all()
    print('DB created exitosamente.')

# para desplegar con gunicorn
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)