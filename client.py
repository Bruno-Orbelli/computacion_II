import socket, sys

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

HOST = sys.argv[1]
PORT = int(sys.argv[2])

s.connect((HOST, PORT))
print(f'Connection established!\n')

greeting = s.recv(4096).decode('ascii')
print(greeting)

while True:
    inp = input('INPUT: ')
    s.send(inp.encode('ascii'))

    if inp.lower() == 'exit':
        break

    resp = s.recv(4096).decode('ascii')
    print(f'\nOUTPUT: {resp}\n')

goodbye = s.recv(4096).decode('ascii')
print(goodbye)
s.close()

