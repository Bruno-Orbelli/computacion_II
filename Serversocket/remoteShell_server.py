import socketserver as ss, argparse as arg
from subprocess import Popen, PIPE

class ThreadingTCPServer(ss.ThreadingMixIn, ss.TCPServer):
    pass

class ForkingTCPServer(ss.ForkingMixIn, ss.TCPServer):
    pass

class TCPShellHandler(ss.BaseRequestHandler):
   
    def handle(self):

       print(f'Conexión establecida con {self.client_address}.\n')
       
       while True: 
        
        self.data = self.request.recv(1024).strip().decode('utf-8')
        print(f'{self.client_address} escribió: "{self.data}"\n')
        terminal = Popen(self.data, stdout = PIPE, stderr = PIPE, shell = True)
        out, err = terminal.communicate()
        
        if self.data == 'exit':
            self.request.sendall(f'GOODBYE\n'.encode("utf-8"))
            print(f'Finalizando conexión con {self.client_address}.\n')
            exit(0)
        
        elif err.decode('utf-8') == '':
            self.request.sendall(f'OK\n\n-----------------------------\n{out.decode("utf-8")}-----------------------------\n'.encode("utf-8"))
        
        else:
            self.request.sendall(f'ERROR\n\n{err.decode("utf-8")}'.encode("utf-8"))

def parse_args():

    parser = arg.ArgumentParser(description = """Recibe un número de puerto y un mecanismo de concurrencia (process o thread). Levanta un servidor
    en la máquina local y en el puerto especificado, donde las solicitudes concurrentes son manejadas a través de la creación de nuevos procesos o
    hilos, según lo indicado.""")
    parser.add_argument('-p', '--port', type = int, default = 0, help = "número del puerto en el que se hostea el servidor")
    parser.add_argument('-c', '--conc', required = True, choices = ['p', 't'], help = """mecanismo de concurrencia empleado por el servidor: multiproceso (p)
    o multihilo (t)""")

    return parser.parse_args()

def main(args):

    HOST, PORT = "localhost", args.port
    options = {
        't': ss.ThreadingTCPServer,
        'p': ss.ForkingTCPServer
    }
    
    with options.get(args.conc)((HOST, PORT), TCPShellHandler) as server:
        print(f'Server "{HOST}" ({server.server_address[0]}) levantado y hosteado en el puerto {server.server_address[1]}.')
        print('Esperando conexiones...\n')
        
        server.serve_forever()

if __name__ == '__main__':
    main(parse_args())