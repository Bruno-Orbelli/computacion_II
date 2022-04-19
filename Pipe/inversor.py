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
            
            line = i.strip()
            
            new_proc = os.fork()
        
            if not new_proc:
                os.close(r)
                w = os.fdopen(w, 'w')
                w.write(f'{line[::-1]}\n')
                w.close()
                os._exit(0)
            
        file.seek(0)
        
        for j in range(len(file.readlines())):
            os.wait()

        if os.getpid():
            os.close(w)
            r = os.fdopen(r, 'r')
            for phrase in r.readlines():
                print(f'{phrase}', end = '')
            r.close()

if __name__ == '__main__':
    main(get_args())