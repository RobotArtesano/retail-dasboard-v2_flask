# Que es un worker?
# Un worker es un proceso que se ejecuta en segundo plano para realizar tareas que pueden ser pesadas o que tardan mucho tiempo, 
# sin bloquear la experiencia del usuario en la aplicación web.

# Consumidor de Redis (RQ/Celery) para tareas en segundo plano: 
# Redis es un almacen en memoria, muy rapido y se usa como sistema de colas para manejar tareas en segundo plano. 
# RQ (Redis Queue) y Celery(Arternativa mas compleja) son dos librerias populares en Python para manejar estas colas de tareas.

# El forecast tarda unos segundos en ejecutarse, por lo que lo hacemos en segundo plano para no bloquear la experiencia del usuario 
# si se bloquea request, se bloquea toda la app, por eso es importante usar tareas en segundo plano para procesos pesados o que tarden mucho tiempo.

# PARA EL PROYECTO RETAIL, USAMOS RQ POR SU SENCILLEZ Y FACIL INTEGRACION CON FLASK, PERO CELERY ES UNA ALTERNATIVA MAS COMPLEJA PERO MAS POTENTE.

from app import create_app, db