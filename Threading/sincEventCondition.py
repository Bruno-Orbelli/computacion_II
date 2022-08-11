import sys, threading as t, argparse as arg, multiprocessing as m, time

def get_args():

    parser = arg.ArgumentParser(description = '''Crea dos hilos: el primero recibe entrada por parte del usuario, y el segundo
    almacena línea a línea la entrada en mayúsculas en el path pasado como argumento. Cuando se introduce "bye" como entrada, el
    programa finaliza.''')
    parser.add_argument('-f', '--file', required = True, help = 'path del archivo en el que se almacena la entrada.')
    args = parser.parse_args()

    return args

def main(args):

    q = m.Queue(1)
    termEvent, newDataEvent, consumedDataEvent  = t.Event(), t.Event(), t.Event()
    h1 = t.Thread(target = sendInput, name ='h1', args = (q, termEvent, newDataEvent, consumedDataEvent))
    h2 = t.Thread(target= writeInput, name = 'h2', args = (q, termEvent, newDataEvent, consumedDataEvent, args.file))

    h1.start()
    h2.start()    
    
    termEvent.wait()

def sendInput(queue, terminationEvent, newDataEvent, consumptionEvent):

    sys.stdin = open(0)
    read = ''

    while True:
        
        print('Ingrese la entrada a ser escrita: ')
        read = sys.stdin.readline()

        if read.lower() != 'bye\n':
            queue.put(read)
            time.sleep(0.00001) # sin el sleep, el set del event ocurre antes que la escritura en queue y genera una excepción
            newDataEvent.set()
            consumptionEvent.wait()
            consumptionEvent.clear()
        
        else:
            terminationEvent.set()
            newDataEvent.set()
            return

def writeInput(queue, terminationEvent, newDataEvent, consumptionEvent, path):
        
    while True:
            
        newDataEvent.wait()
        newDataEvent.clear()
            
        with open(path, 'a') as file:
            
            if not terminationEvent.is_set():            
                line = queue.get_nowait()
                file.write(line.upper())
                print('Se escribio: {}.\n'.format(line.upper().rstrip("\n")))
                consumptionEvent.set()
            
            else:
                return

if __name__ == '__main__':
    main(get_args())


    


    
