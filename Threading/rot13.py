import multiprocessing as mult
import threading
import sys

def main():
    
    q = mult.Queue()
    p1, p2 = mult.Pipe(False)
    
    t1 = threading.Thread(target = readInput, name = 't1', args = (q, p2))
    t2 = threading.Thread(target = cypherInput, name = 't2', args = (q, p1))

    t1.start()
    t2.start()
    
    while t1.is_alive() or t2.is_alive():
        pass

def readInput(queue, pipe):
    
    sys.stdin = open(0)
    print('Ingrese una línea: ')
    
    for line in sys.stdin:
        
        pipe.send(line)
        if line == '\n':
            return
        
        print(f'Salida codificada: {queue.get()}\n')
        print('Ingrese una línea: ')

def cypherInput(queue, pipe):

    alpha = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
    
    while True:
        
        inp = pipe.recv()
        
        if inp != '\n':
            
            aux = []
            inp = inp.split()
            
            for elem in inp:   
                
                try:
                    for char in elem:
  
                        if char.upper() not in alpha:
                            raise Exception('Al menos uno de los caracteres ingresados no es una letra.')

                        if char == '\n':
                            continue
                        
                        elif char.isupper():
                            aux.append(alpha[(alpha.index(char) + 13) % 26])
                        
                        else:
                            aux.append(alpha[(alpha.index(char.upper()) + 13) % 26].lower())
                
                except Exception as e:
                    print(f'ERROR: {e}')
                
                aux.append(' ')
            
            inp = ''.join(char for char in aux)
            queue.put(inp)

        else:
            return

if __name__ == '__main__':
    main()