import socket as s, sys, argparse as arg

def parse_args():

    parser = arg.ArgumentParser(description = """Recibe la dirección IP y el número de puerto en el que está hosteado el servidor shell,
    fungiendo como cliente.""")
    parser.add_argument('-ip', required = True, help = "dirección IP en la que se hostea el servidor")
    parser.add_argument('-p', '--port', required = True, help = "puerto de la máquina en el que se hostea el servidor")

    return parser.parse_args()

def main(args):
    
    with s.socket(s.AF_INET, s.SOCK_STREAM) as sock:
    
        sock.connect((args.ip, int(args.port)))
        print(f'\nConexión establecida!\n')
        print('SHELL REMOTO'.center(50, '=') + '\n')

        while True:
            command = input('> ')
            sock.sendall(f'{command}\n'.encode('utf-8'))

            if command == 'exit':
                break

            resp = sock.recv(1024).decode('utf-8')
            print(f'\n{resp}')

        goodbye = sock.recv(1024).decode('utf-8')
        print(f'\n{goodbye}')

if __name__ == '__main__':
    main(parse_args())