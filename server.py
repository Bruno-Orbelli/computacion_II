import socket, sys, os, signal

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

HOST = socket.gethostname()
PORT = int(sys.argv[1])

s.bind((HOST, PORT))

s.listen(1)

signal.signal(signal.SIGCHLD, signal.SIG_IGN)

print('Awaiting connection...\n')

while True:
    
    cs, add = s.accept()

    print(f'Connection established!\nIP: {add}\n')
    
    greeting = 'Welcome to MAYUSCULIZADOR'.center(40, '=') + '\nTo begin, please input any text...\n...the server will output your entry in all caps!\n'
    greeting += 'If you wish to disconnect, type "exit".\n'
    cs.send(greeting.encode('ascii'))

    cpid = os.fork()
    
    if not cpid:
        while True:
            inp = cs.recv(4096).decode('ascii')

            if inp.lower() == 'exit':
                cs.send('\nGoodbye!'.encode())
                print(f'Ending connection with {add}.\n')
                cs.close()
                exit()
            
            response = inp.upper()
            cs.send(response.encode('ascii'))

            



    