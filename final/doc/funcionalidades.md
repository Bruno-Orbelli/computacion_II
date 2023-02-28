## Cliente

### CLI / GUI
+ Recibe la lista de documentos a convertir y provee opciones de conversión al cliente (formato destino, exportar/no exportar índices, parte de la base de datos/totalidad de la base de datos).

### Sistema de lectura/escritura asíncrono
+ Se conecta a las bases de datos, utilizando AsyncIO para acceder a los archivos solicitados y escribir la información convertida en forma asíncrona.

### Receptor y emisor de datos
+ Emite y recibe la información de las bases e información de control adicional desde y hacia el servidor, en la forma de flujo de datos.

***

## Servidor


### Receptor y emisor de datos
+ Emite y recibe la información de las bases e información de control adicional desde y hacia el cliente, en la forma de flujo de datos.

### Conversor
+ Bloque de código principal del servidor.
+ Identifica la/s operación/es a realizar, envía las tareas en forma de mensajes a una cola de tareas e interacúa con el logger para almacenar la actividad ocurrida.

### Broker de mensajes
+ Implementa una cola de tareas utilizando Reddis, de la cual consumen los diferentes workers disponibles en el servidor.
+ Cada tarea se corresponde con una tabla/colección de la base de datos a convertir.

### Pool de workers
+ Consume los mensajes del broker, efectuando las conversiones en forma paralela sobre las diferentes tablas/colecciones.
+ Utiliza Celery.

### Módulo de operaciones
+ Contiene las diferentes operaciones de conversión que los workers pueden realizar sobre una tabla/colección.

### Logger de interacciones
+ Logea, en forma asincrónica, la actividad del servidor.
+ Implementa una cola de loggeos pendientes, con la información de los eventos a ser escritos en el log.