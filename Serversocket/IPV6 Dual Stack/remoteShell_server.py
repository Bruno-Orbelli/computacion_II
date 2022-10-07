import socketserver as ss, argparse as arg, pickle as p, sys, socket, threading
from subprocess import Popen, PIPE

class ThreadingTCPServerIPV4(ss.ThreadingMixIn, ss.TCPServer):
    pass

class ForkingTCPServerIPV4(ss.ForkingMixIn, ss.TCPServer):
    pass

class ThreadingTCPServerIPV6(ss.ThreadingMixIn, ss.TCPServer):
    address_family = socket.AF_INET6
    pass

class ForkingTCPServerIPV6(ss.ForkingMixIn, ss.TCPServer):
    address_family = socket.AF_INET6
    pass

class TCPShellHandler(ss.BaseRequestHandler):
    
    def handle(self):

        print(f'Conexión establecida con {self.client_address}.\n')
       
        while True:   
            
            try:
                self.data = p.loads(self.request.recv(4096)).strip()
                print(f'{self.client_address} escribió: "{self.data}"\n')
                terminal = Popen(self.data, stdout = PIPE, stderr = PIPE, shell = True)
                out, err = terminal.communicate()
                
                if self.data == 'exit':
                    self.request.sendall(p.dumps(f'GOODBYE\n'))
                    print(f'Finalizando conexión con {self.client_address}.\n')
                    exit(0)
                
                elif err.decode('utf-8') == '':
                    out = out.decode("utf-8")
                    msg = p.dumps(f'OK\n\n-----------------------------\n{out}-----------------------------\n', protocol = p.HIGHEST_PROTOCOL)
                    self.request.sendall(msg)
                
                else:
                    msg = p.dumps(f'ERROR\n\n{err.decode("utf-8")}')
                    self.request.sendall(msg)
                
                print(f'Enviando {sys.getsizeof(msg)}B a {self.client_address}.\n')
            
            except (EOFError, ConnectionResetError):
                print(f'Conexión finalizada inesperadamente con {self.client_address}.\n')
                exit(0)

def parse_args():

    parser = arg.ArgumentParser(description = """Recibe un número de puerto y un mecanismo de concurrencia (process o thread). Levanta un servidor
    en la máquina local y en el puerto especificado, donde las solicitudes concurrentes son manejadas a través de la creación de nuevos procesos o
    hilos, según lo indicado.""")
    parser.add_argument('-p', '--port', type = int, default = 0, help = "número del puerto en el que se hostea el servidor")
    parser.add_argument('-c', '--conc', required = True, choices = ['p', 't'], help = """mecanismo de concurrencia empleado por el servidor: multiproceso (p)
    o multihilo (t)""")
    parser.add_argument('-v', '--verbose', action = "store_true", help = "muestra información adicional de los paquetes y el funcionamiento del servidor.")

    return parser.parse_args()

def run_server(servertype, sock):
    
    with servertype(sock[4], TCPShellHandler) as server:
        print(f'Server "localhost" ({server.server_address[0]}) levantado y hosteado en el puerto {server.server_address[1]}.')
        print('Esperando conexiones...\n')
            
        server.serve_forever()
                
def main(args):

    PORT = args.port
    options = {
        't': (ThreadingTCPServerIPV4, ThreadingTCPServerIPV6),
        'p': (ForkingTCPServerIPV4, ForkingTCPServerIPV6)
    }
    
    sockets = (info for info in socket.getaddrinfo(None, PORT, 0, socket.SOCK_STREAM, 0)[::-1])
    ss.TCPServer.allow_reuse_address = True
    
    for servertype, sock in zip(options.get(args.conc), sockets):
        try:
            threading.Thread(target= run_server, args= (servertype, sock)).start()

        except KeyboardInterrupt:
            print('\nApagando el servidor...\n')
            exit(0)

if __name__ == '__main__':
    main(parse_args())