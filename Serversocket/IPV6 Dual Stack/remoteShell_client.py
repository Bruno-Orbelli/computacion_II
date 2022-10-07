import socket as s, argparse as arg, pickle as p, sys

def parse_args():

    parser = arg.ArgumentParser(description = """Recibe la dirección IP y el número de puerto en el que está hosteado el servidor shell,
    fungiendo como cliente.""")
    parser.add_argument('-ip', required = True, help = "dirección IP en la que se hostea el servidor")
    parser.add_argument('-p', '--port', required = True, help = "puerto de la máquina en el que se hostea el servidor")

    return parser.parse_args()

def main(args):
    
    with s.socket(s.AF_INET6, s.SOCK_STREAM) as sock6, s.socket(s.AF_INET, s.SOCK_STREAM) as sock4:
        
        try:
            sock4.connect((args.ip, int(args.port)))
            sock = sock4
        
        except s.gaierror:
            sock6.connect((args.ip, int(args.port)))
            sock = sock6

        print(f'\nConexión establecida!\n')
        print('SHELL REMOTO'.center(50, '=') + '\n')

        while True:
            try:
                command = input('> ')
                sock.sendall(p.dumps(f'{command}\n'))

                if command == 'exit':
                    break

                out = b''
                
                while True:
                    
                        packet = sock.recv(4096)
                        out += packet

                        if sys.getsizeof(packet) <= 4096:
                            break

                out = p.loads(out)
                out += '\n' if out[-1::] != '\n' else ''
                print(f'\n{out}')
            
            except (KeyboardInterrupt, EOFError) as e:
                if isinstance(e, KeyboardInterrupt):
                    print('\nCerrando cliente...\n')
                        
                else:
                    print('\nLa conexión con el servidor se ha interrumpido repentinamente.\n')
                       
                exit(0)

        goodbye = p.loads(sock.recv(4096))
        print(f'\n{goodbye}')

if __name__ == '__main__':
    main(parse_args())