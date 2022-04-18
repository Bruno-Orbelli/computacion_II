import os, argparse

def get_args():

    parser = argparse.ArgumentParser(description = '''Recibe un archivo de texto e invierte cada una de sus líneas, creando un proceso hijo para cada una de 
    las mismas y mostrando la salida por pantalla.''')
    parser.add_argument('-f', '--file', required = True, help = 'el path del archivo de texto a ser invertido')

    args = parser.parse_args()
    return args

def main(args):

    r, w = os.pipe()
    
    with open(args.file, 'r+') as file:
        
        for i in file.readlines():
            
            new_proc = os.fork()
        
            if not new_proc:
                w = os.fdopen(w, 'w')
                w.write(i[-1])
                print(i)
                w.close()
                os._exit(0)
            
        file.seek(0)
        
        for j in range(len(file.readlines())):
            os.wait()

        r = os.fdopen(r, 'r')
        print(r)
        print(f'{r.readlines()}')
        r.close()

if __name__ == '__main__':
    main(get_args())