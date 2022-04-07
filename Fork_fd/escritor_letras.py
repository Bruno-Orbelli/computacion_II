import argparse
import os
import time

def get_args():

    parser = argparse.ArgumentParser(description = '''Recibe tres argumentos: una cant. de procesos hijo a crear, una cant. de repeticiones y un path.
    Crea un archivo de texto en el path especificado (si ya existe, lo sobreescribe) en el que cada proceso hijo, que está asociado a una letra (el
    primero con la letra "A", el segundo con la "B", y así sucesivamente), escribre su letra tantas veces como indique la cant. de repeticiones.''')
    parser.add_argument('-n', '--numprocesses', required = True, type = int, help = 'la cant. de procesos hijo a crear')
    parser.add_argument('-r', '--repetitions', required = True, type = int, help = 'la cant. de veces que cada proceso hijo escribirá su letra')
    parser.add_argument('-f', '--file', required = True, help = 'el path del documento donde se escribirán las letras')
    parser.add_argument('-v', '--verbose', action = 'store_true', help = '''modo verboso, imprime un mensaje cada vez que un proceso escribe una letra
    indicando su PID y la letra escrita''')

    args = parser.parse_args()
    return args

def main(args):

    alfa = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'Ñ', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
    
    with open(args.file, 'w+') as file:
        
        for i in range(args.numprocesses):
            new_proc = os.fork()
            
            if new_proc == 0:
                letra = alfa[i % len(alfa)]  #  si se indica un número de procesos hijo mayor que la cant. de letras del abecedario, se les vuelve a asignar la A,B,...
                for j in range(args.repetitions):
                    if args.verbose:
                        print(f'Proceso {os.getpid()} escribiendo letra "{letra}"')
                    file.write(letra)
                    file.flush()
                    time.sleep(1)
                os._exit(0)
        
        for j in range(args.numprocesses):
            os.wait()
        
        file.seek(0)
        print(file.read())

if __name__ == '__main__':
    main(get_args())