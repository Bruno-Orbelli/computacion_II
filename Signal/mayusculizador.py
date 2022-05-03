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

def writeToMemory(s, f):

    mapped.seek(0)
    mapped.resize(len(uInput))
    mapped.write(bytes(uInput, encoding = 'UTF-8'))
    sg.signal(sg.SIGUSR1, sg.SIG_IGN)
    os.kill(ppid, sg.SIGUSR1)

def writeToFile(s, f):

    with open(file, 'a+') as route:
        route.write(uInput)

def main(args):
    
    sg.signal(sg.SIGUSR1, sg.SIG_IGN)
    
    with m.mmap(-1, 1024) as memory:   
        
        global mapped, file
        global h1, h2, ppid
        mapped, file = memory, args.file
        ppid = os.getpid()
        print(ppid)
        
        for line in s.stdin:
            
            global uInput
            uInput = line
            
            h1 = os.fork()
            
            if not h1:
                sg.signal(sg.SIGUSR1, writeToMemory) 
                os.kill(h1, sg.SIGUSR1)
                sg.sigwait([sg.SIGUSR1])
                os._exit(0)

            if os.getpid() == ppid:
                mapped.seek(0)
                readLine = memory.readline()
                print(readLine.decode(encoding = 'UTF-8'))
                h2 = os.fork()

            if not h2:
                sg.signal(sg.SIGUSR1, writeToFile)
                os.kill(h2, sg.SIGUSR1)
                os._exit(0)

if __name__ == '__main__':
    main(parse_args())
