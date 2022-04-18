import os, argparse

def get_args():

    parser = argparse.ArgumentParser(description = '''Recibe un archivo de texto e invierte cada una de sus líneas, creando un proceso hijo para cada una de 
    las mismas y mostrando la salida por pantalla.''')
    parser.add_argument('-f', '--file', required = True, help = 'el path del archivo de texto a ser invertido')

    args = parser.parse_args()
    return args

def main(args):

    r, w = os.pipe()
    
    with open(args.file, 'r') as file:
        
        for i in file.readlines():
            new_proc = os.fork()
        
            if new_proc == 0:
                os.fdopen(r, 'r')
                os.fdopen(w, 'w')
                w.write(i[-1])
                w.close()
                os._exit(0)
            
        for j in range(len(file.readlines())):
            os.wait()
        
        rp.readline