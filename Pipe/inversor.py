from binhex import LINELEN
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

    with open(args.file, 'r+') as file:
        
        invLines, linesAmount = 0, len(file.readlines())
        file.seek(0)

        for i in file.readlines():
            
            line = i.strip()
            new_proc = os.fork()
            print(new_proc)
        
            if new_proc:
                if not invLines:                
                    wc = os.fdopen(wc, 'w')
                
                wc.write(line)
                invLines += 1
                
                if invLines == linesAmount:
                   wc.close()
            
            else:
                try:
                    os.close(wc)
                except TypeError:
                    wc.close()
                
                rc = os.fdopen(rc, 'r')
                toRead = rc.read()
                rc.close()
                
                wp = os.fdopen(wp, 'w')
                wp.write(f'{toRead[::-1]}\n')
                wp.close()
                os._exit(0)
            
        for j in range(linesAmount):
            os.wait()
            print('hijo terminado')

        os.close(wp)
        rp = os.fdopen(rp, 'r')
        for phrase in rp.readlines():
            print(f'{phrase}')
        rp.close()

if __name__ == '__main__':
    main(get_args())