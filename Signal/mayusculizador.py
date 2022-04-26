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
    
    with open('shared_memory.txt', 'w+') as mem:
        
        memory = m.mmap(mem.fileno(), 1024)
        sg.signal(sg.SIGUSR1, ouput_or_store())
        
        cpid = []
        for _ in range(2):
            proc = os.fork()
            if not proc:
                cpid.append(os.getpid())
        
        for line in s.stdin:
            
            if os.getpid() == cpid[0]: # proceso H1
                memory.write(bytes(line))
                sg.raise_signal(sg.SIGUSR1)


        
