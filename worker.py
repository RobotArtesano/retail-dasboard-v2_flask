# Que es un worker?
# Un worker es un proceso que se ejecuta en segundo plano para realizar tareas que pueden ser pesadas o que tardan mucho tiempo, 
# sin bloquear la experiencia del usuario en la aplicación web.

# Consumidor de Redis (RQ/Celery) para tareas en segundo plano: 
# Redis es un almacen en memoria, muy rapido y se usa como sistema de colas para manejar tareas en segundo plano. 
# RQ (Redis Queue) o Celery(Arternativa mas compleja) son dos librerias populares en Python para manejar estas colas de tareas.

# El forecast tarda unos segundos en ejecutarse, por lo que lo hacemos en segundo plano para no bloquear la experiencia del usuario 
# si se bloquea request, se bloquea toda la app, por eso es importante usar tareas en segundo plano para procesos pesados o que tarden mucho tiempo.

# PARA EL PROYECTO RETAIL, USAMOS RQ POR SU SENCILLEZ Y FACIL INTEGRACION CON FLASK, AUNQUE CELERY ES UNA ALTERNATIVA MAS COMPLEJA PERO MAS POTENTE.

import os
import redis
from rq import Worker, Queue
from app import create_app

# 1. Definimos que coloa va a escuchar el worker, en este caso 'forecast-tasks', que es la cola donde se encolan las tareas de pronóstico de ventas.
listen = ['forecast-tasks']

# 2. Conectamos a Redis, usando la URL de conexión definida en la configuración de la aplicación Flask.
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
conn = redis.from_url(redis_url)

if __name__ == '__main__':
    # 3. Empaquetamos el trabajador dentro del contexto de Flask para que tenga acceso a la configuración y a las extensiones de la aplicación.
    # Esto es VITAL para que el worker pueda leer tu BD, usar la configuración de Flask, etc.
    app = create_app()
    with app.app_context():

        # 3.5 Forma moderna de pasar la conexion a al queue y al worker sin necesidad de importar 'Connection'
        # En versiones anteriores de RQ, se usaba 'with Connection(conn):' para establecer la conexión a Redis para el worker.
        colas = [Queue(nombre_cola, connection=conn) for nombre_cola in listen]

        # 4. Creamos una conexión a Redis y un worker que escuche la cola 'forecast-tasks'.
        worker = Worker(colas, connection=conn)
        # 5. Iniciamos el worker para que comience a escuchar y procesar tareas en la cola.
        print("Worker iniciado, escuchando la cola 'forecast-tasks'...")
        worker.work()