from multiprocessing import Pool
import math
import argparse

def get_args():

    parser = argparse.ArgumentParser(description = '''Recibe un número entero p, una ruta con una matriz de valores y una operación de cálculo de entre
    tres disponibles (raiz, pot o log). Se crea un pool de p procesos, que efectúan la operación especificada, cada uno trabajando en paralelo sobre una
    de las filas de la matriz.''')
    
    parser.add_argument('-p', '--processes', required = True, help = 'nº de procesos a crear', type = int)
    parser.add_argument('-f', '--file', required = True, help = 'path del txt con la matriz')
    parser.add_argument('-c', '--calc', required = True, choices = ['raiz', 'pot', 'log'], help = '''operacion a efectuar: raiz cuadrada (raiz), 
    potencia cuadrada (pot) o logaritmo decimal (log)''')
    
    args = parser.parse_args()
    return args

def raiz(num):
    return round(math.sqrt(int(num)), 5)

def pot(num):
    return round(int(num) ** 2, 5)

def log(num):
    return round(math.log10(int(num)), 5)

def main(args):

    matriz = []
    
    with open(args.file, 'r') as file:
        
        for line in file.readlines():
            row = line.strip("\n ").split(',')
            matriz.append(row)
        
        if len(matriz) < int(args.processes):
            for j in range(int(args.processes) - len(matriz)):
                matriz.append([])
        
    with Pool(int(args.processes)) as p:
        
        for i in range(len(matriz)):
            function = {'raiz': raiz, 'pot': pot, 'log': log}[args.calc]
            matriz[i] = p.map(function, matriz[i])
        
    for row in matriz:
        output = ', '.join(str(elem) for elem in row)
        print(output)

if __name__ == '__main__':
    main(get_args())
