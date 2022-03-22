import argparse

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description = '''Toma el nombre de dos archivos y copia el contenido de uno dentro del otro.
    Si el archivo de destino ya existe, procede a sobreescribir su contenido.''')
    parser.add_argument('-i', '--inicial', help = 'archivo original (del cual se copia el contenido)', required = True)
    parser.add_argument('-o', '--final', help = 'archivo destino (en el cual se copia/sobreescribe el contenido)', required = True)
    args = parser.parse_args()

    try:
        with open(args.inicial, 'r') as file1:
            with open(args.final, 'w') as file2:
                for x in file1:
                    file2.write(x)
    except Exception as e:
        print('error: {}'.format(e))