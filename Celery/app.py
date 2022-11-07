import argparse, funciones

def get_args():

    parser = argparse.ArgumentParser(description = '''Recibe una ruta con una matriz de valores y una operación de cálculo de entre
    tres disponibles (raiz, pot o log), creando un pool de Celery con workers que efectúan la operación especificada
    en paralelo sobre las filas de la matriz.''')
    
    parser.add_argument('-f', '--file', required = True, help = 'path del txt con la matriz')
    parser.add_argument('-c', '--calc', required = True, choices = ['raiz', 'pot', 'log'], help = '''operacion a efectuar: raiz cuadrada (raiz), 
    potencia cuadrada (pot) o logaritmo decimal (log)''')
    
    args = parser.parse_args()
    return args

def main(args):

    matriz = []
    
    with open(args.file, 'r') as file:
        
        for line in file.readlines():
            row = line.strip("\n ").split(',')
            matriz.append(row)
    
    for i, row in enumerate(matriz):
        for j, value in enumerate(row):
            function = {'raiz': funciones.raiz, 'pot': funciones.pot, 'log': funciones.log}[args.calc]
            result = function.delay(float(value))
            matriz[i][j] = result.get()
        
    print("\n", end="")
    
    for row in matriz:
        output = ', '.join(str(elem) for elem in row)
        print(output)

    print("\n", end="")

if __name__ == '__main__':
    main(get_args())