import argparse
import subprocess
import datetime
import sys

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = '''Ejecuta un comando y almacena su salida en el path especificado, además de almacenar información de 
    ejecución del comando en un segundo archivo.''', )
    parser.add_argument('-c', '--command', required = True, help = "el nombre del comando a ejecutar")
    parser.add_argument('-f', '--file', required = True, help = "el archivo en el cual se almacena la salida")
    parser.add_argument('-l', '--logfile', required = True, help = '''el archivo en el cual se almacena info de ejecución (fecha y hora + ejecución correcta /
    incorrecta del comando)''')
    args = parser.parse_args()

    with open(args.file, 'a') as file:
        with open(args.logfile, 'a') as log:
            err = ''
            p1 = subprocess.Popen(args.command.split(), stdout = file, stderr = err)    
            if err == '':
                p2 = subprocess.Popen(['echo', '{}: Comando "{}" ejecutado correctamente.'.format(datetime.datetime.now(), args.command)], stdout = log)
            else:
                p3 = subprocess.Popen(['echo', '{}: {}'.format(datetime.datetime.now(), 1)], stdout = log)
   
