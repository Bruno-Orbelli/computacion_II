import asyncio as a, argparse as arg, pickle as p, sys
from subprocess import Popen, PIPE
  
async def handle(reader, writer):

    addr = writer.get_extra_info('peername')
    print(f'Conexión establecida con {addr}.\n')
      
    while True:   
            
        try:
            data = await reader.read(4096)
            data = p.loads(data).strip()
            
            print(f'{addr} escribió: "{data}"\n')
            terminal = Popen(data, stdout = PIPE, stderr = PIPE, shell = True)
            out, err = terminal.communicate()
                    
            if data == 'exit':
                writer.write(p.dumps(f'GOODBYE\n'))
                print(f'Finalizando conexión con {addr}.\n')
                return
                    
            elif err.decode('utf-8') == '':
                out = out.decode("utf-8")
                msg = p.dumps(f'OK\n\n-----------------------------\n{out}-----------------------------\n', protocol = p.HIGHEST_PROTOCOL)
                writer.write(msg)
                await writer.drain()
                    
            else:
                msg = p.dumps(f'ERROR\n\n{err.decode("utf-8")}')
                writer.write(msg)
                await writer.drain()
                    
            print(f'Enviando {sys.getsizeof(msg)}B a {addr}.\n')
                
        except (EOFError, ConnectionResetError):
            print(f'Conexión finalizada inesperadamente con {addr}.\n')
            return

def parse_args():

    parser = arg.ArgumentParser(description = """Recibe un número de puerto y un mecanismo de concurrencia (process o thread). Levanta un servidor
    en la máquina local y en el puerto especificado, donde las solicitudes concurrentes son manejadas a través de la creación de nuevos procesos o
    hilos, según lo indicado.""")
    parser.add_argument('-p', '--port', type = int, default = 0, help = "número del puerto en el que se hostea el servidor")

    return parser.parse_args()

async def main(args):

    HOST, PORT = "localhost", args.port
    server = await a.start_server(handle, HOST, PORT)
    
    async with server:
        print(f'Server "{HOST}" levantado y hosteado en el puerto {PORT}.')
        print('Esperando conexiones...\n')
        
        try:
            await server.serve_forever()
        
        except KeyboardInterrupt:
            print('\nApagando el servidor...\n')

if __name__ == '__main__':
    a.run(main(parse_args()))