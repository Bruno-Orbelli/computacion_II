'''Etapa 1:

El programa deberá crear un segmento de memoria compartida anónima, y generar dos hijos: H1 y H2

El H1 leerá desde el stdin línea por línea lo que ingrese el usuario.

Cada vez que el usuario ingrese una línea, H1 la almacenará en el segmento de memoria compartida, y enviará la señal USR1 al proceso padre.

El proceso padre, en el momento en que reciba la señal USR1 deberá mostrar por pantalla el contenido de la línea ingresada por el H1 en la memoria compartida, y deberá notificar al H2 usando la señal USR1.

El H2 al recibir la señal USR1 leerá la línea desde la memoria compartida la línea, y la almacenará en mayúsculas en el archivo pasado por argumento (path_file).'''

import argparse as a
import sys as s
import signal as sg
import mmap as m
import os

def parse_args():
    
    parser = a.ArgumentParser(description = '''Recibe un path como argumento y genera dos procesos hijo, donde el primer hijo lee linea
    a línea la entrada por consola del usuario y la almacena en un segmento de memoria compartida. Por cada línea ingresada, el padre 
    muestra en pantalla su contenido y el segundo hijo toma la línea de la memoria y la almacena en mayúscula en el path especificado.''')
    parser.add_argument('-f', '--file', required = True, help = 'el path del file en el que se almacena la línea en mayúscula')

    args = parser.parse_args()
    return args

def main(args):
    
    with m.mmap(-1, 1024) as memory:   
        
        sg.signal(sg.SIGUSR1, sg.SIG_DFL) 
        ppid, cpid = os.getpid(), []
        r, w = os.pipe()     

        for _ in range(2):
            if os.getpid() == ppid:
                proc = os.fork()
                cpid.append(proc)

        if proc:
            w = os.fdopen(w, 'w')
            print(cpid)
            w.write(f'{cpid}')
            w.close()
        
        else:
            os.close(w)
            r = os.fdopen(r, 'r')
            cpid = r.read()
            print(cpid)
            r.close()
        
        for line in s.stdin:

            print(line)

            if os.getpid() == cpid[0]: # proceso H1
                print('hijo1')
                memory.write(bytes(line))
                sg.raise_signal(sg.SIGUSR1)
                    
            elif os.getpid() == cpid[1]: # proceso H2
                print('hijo2')
                for _ in range(2):
                    sg.sigwait([sg.SIGUSR1])
                        
                readLine = memory.readline()
                    
                with open(args.file, 'w') as file:  
                    file.write(readLine.upper())
                    
            else: # proceso padre
                sg.sigwait([sg.SIGUSR1])
                readLine = memory.readline()
                print(readLine.decode(encoding = 'UTF-8'))
                sg.raise_signal(sg.SIGUSR1)

if __name__ == '__main__':
    main(parse_args())






        
