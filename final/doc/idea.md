# Proyecto final de Computación II: ConverSQL
*Orbelli, Bruno - Legajo 60131*

***

## Idea
+ Desarrollar una aplicación cliente-servidor que permita la migración entre diferentes formatos de base de datos (*.JSON*, *.CSV* para bases de datos **NoSQL**, *.DB*, *.EDB* y demás formatos para bases **SQL**), manteniendo las restricciones originales de los modelos de datos y proporcionando, también, opciones para exportar índices o porciones de la base.

***

## Funcionamiento
+ El cliente provee tanto una interfaz de línea de comandos como una interfaz gráfica, donde se puede especificar o cargar, como argumentos, los paths de los archivos de base de datos. 
+ Los archivos especificados son leídos en forma asincrónica en el extremo cliente y enviados como un flujo de datos al extremo servidor, junto con el/los tipos de conversión a realizar y las restricciones adicionales a tener en cuenta. 
+ El servidor utiliza un pool de workers para trabajar en forma paralela en las diferentes tablas/colecciones y convierte los archivos adecuadamente, loggeando cada interacción. 
+ El flujo de datos convertido es enviado al extremo cliente, que los escribe como un nuevo archivo de base de datos.

***

## Herramientas/librerías a utilizar
+ Celery
+ Reddis
+ Docker
+ Multiprocessing
+ AsyncIO
+ Argparse
+ Socket
+ Pickle