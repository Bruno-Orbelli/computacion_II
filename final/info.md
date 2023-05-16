## Decisiones de diseño principales
- Para este proyecto, se eligió utilizar **asyncio** en el sistema de lectura/escritura para poder hacer mejor uso de los recursos (por parte del cliente) en los tiempos 'muertos' de las operaciones de lectura y escritura de base de datos.
- También se hace uso de los **sockets asíncronos** en los extremos de emisión y recepción de datos para permtir un envío de datos y un procesamiento más fluído por parte del servidor y del cliente (evitando que ambos se bloqueen esperando a que el otro termine de enviar/procesar la información).
- Se utiliza **pickle** como serializador para permitir el envío de diccionarios de Python como tales (cuyo formato es utilizado en las solicitudes) a lo largo de ambos extremos. Dado que, en algunos casos, la información enviada contiene otros objetos específicos de Python (*datetime*, *Decimal*, entre otros) es preferible el uso de **pickle** (en esta instancia) en lugar de **json**.
- Se emplea **celery** como motor de procesamiento de las solicitudes, ya que ahorra la implementación de colas propias para las solicitues a procesar, permite el procesamiento en paralelo de múltiples solicitudes y alivia la carga del servidor, desacoplando el procesamiento de los requests de la recepción y envio de las mismas y permitiendo que ambos funcionen en forma independiente.
- Finalmente, se utiliza un sistema de logeo asíncrono, basado en objectos **Queue** y **asyncio** para evitar cargar el servidor en los momentos de alta demanda/procesamiento elevado, logeando las interacciones en instantes 'muertos' del servidor.