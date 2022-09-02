import socket as s, argparse as arg, pickle as p, time

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
            sock.sendall(p.dumps(f'{command}\n'))

            if command == 'exit':
                break

            out = ''
            
            while True:
                packet = p.loads(sock.recv(4096))
            
                out += packet

                if (packet[-3::] == 'EOF'): 
                    out = out[:-3:]
                    break

                elif packet[:5:] == 'ERROR':
                    break

                out += '\n' if out[-1::] != '\n' else ''
                
            print(f'\n{out}')

        goodbye = p.loads(sock.recv(4096))
        print(f'\n{goodbye}')

if __name__ == '__main__':
    main(parse_args())