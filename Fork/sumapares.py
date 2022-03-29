import argparse
import os

def main():
    
    # Parser con argumentos

    parser = argparse.ArgumentParser(description = '''Crea tantos procesos hijo como se indique y devuelve la suma de todos los números pares entre 0
    y el PID, junto con el PPID.''')
    parser.add_argument('-n', '--numero', help = 'la cant. de procesos hijo a ser creados', required = True, type = int)
    parser.add_argument('-v', '--verbose', help = 'modo verboso, muestra un mensaje de inicio y de finalización de cada proceso hijo')
    args = parser.parse_args()

    # Bucle para ejecutar n veces
    # If/else para proceso padre/hijo
    # Bucle para la suma

    for i in range(args.numero):


if __name__ == '__main__':
    
