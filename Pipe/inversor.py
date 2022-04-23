import os, argparse

def get_args():

    parser = argparse.ArgumentParser(description = '''Recibe un archivo de texto e invierte cada una de sus líneas, creando un proceso hijo para cada una de 
    las mismas y mostrando la salida por pantalla.''')
    parser.add_argument('-f', '--file', required = True, help = 'el path del archivo de texto a ser invertido')

    args = parser.parse_args()
    return args

def main(args):
    
    rp, wp = os.pipe() # pipe del proc. padre
    rc, wc = os.pipe() # pipe del proc. hijo

    with open(args.file, 'r') as file:
         
        for _ in range(len(file.readlines())):
            
            new_proc = os.fork()
                   
            if not new_proc:
                # lectira del hijo
                os.close(wc) 
                rc = os.fdopen(rc, 'r')
                toInvert = rc.read()
                rc.close()
                
                #escritura del hijo
                os.close(rp)
                wp = os.fdopen(wp, 'w')
                wp.write(f'{toInvert[::-1]}\n')
                wp.close()
                os._exit(0)
               
        # escritura del padre
        os.close(rc)
        wc = os.fdopen(wc, 'w')
        file.seek(0)
        for line in file.readlines():
            wc.write(f'{line.strip()}\n')
        wc.close()
        
        # lectura del padre
        os.close(wp)
        rp = os.fdopen(rp, 'r')
        for phrase in rp.readlines():
            if phrase != '\n':
                print(phrase.strip())
        rp.close()

        file.seek(0)
        
        for j in range(len(file.readlines())):
            os.wait()

if __name__ == '__main__':
    main(get_args())