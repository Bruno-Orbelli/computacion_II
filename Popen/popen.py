import argparse
import subprocess
import datetime

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = '''Ejecuta un comando y almacena su salida en el path especificado, además de almacenar información de 
    ejecución del comando en un segundo archivo.''', )
    parser.add_argument('-c', '--command', required = True, help = "el nombre del comando a ejecutar")
    parser.add_argument('-f', '--file', required = True, help = "el archivo en el cual se almacena la salida")
    parser.add_argument('-l', '--logfile', required = True, help = '''el archivo en el cual se almacena info de ejecución (fecha y hora + ejecución correcta/
    incorrecta del comando)''')
    args = parser.parse_args()

    try:
        p1 = subprocess.Popen([args.command], stdout = subprocess.PIPE)
        
        p3 = subprocess.Popen(['echo', '"{}: Comando {} ejecutado correctamente.'.format(datetime.now(), args.command), '>>', args.logfile])
    except OSError as e:
        p3 = subprocess.Popen(['echo', '"{}: Comando {} ejecutado correctamente.'.format(datetime.now(), e), '>>', args.logfile])
