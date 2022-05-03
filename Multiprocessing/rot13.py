import multiprocessing as mult
import sys

def main():
    
    q = mult.Queue()
    con1, con2 = mult.Pipe(False)
    
    h1 = mult.Process(target = readInput, name = 'h1', args = (q, con2))
    h2 = mult.Process(target = cypherInput, name = 'h2', args = (q, con1))

    h1.start()
    h2.start()
    
    while h1.is_alive() or h2.is_alive():
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
                
                for char in elem:
  
                    if char == '\n':
                        continue
                    
                    elif char.isupper():
                        aux.append(alpha[(alpha.index(char) + 13) % 26])
                    
                    else:
                        aux.append(alpha[(alpha.index(char.upper()) + 13) % 26].lower())
                
                aux.append(' ')
            
            inp = ''.join(char for char in aux)
            queue.put(inp)

        else:
            return

if __name__ == '__main__':
    main()