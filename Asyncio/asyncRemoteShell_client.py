import asyncio as a, argparse as arg, pickle as p, sys

def parse_args():

    parser = arg.ArgumentParser(description = """Recibe la dirección IP y el número de puerto en el que está hosteado el servidor shell,
    fungiendo como cliente.""")
    parser.add_argument('--host', required = True, help = "dirección IP o nombre de host del servidor")
    parser.add_argument('-p', '--port', required = True, help = "puerto de la máquina en el que se hostea el servidor")

    return parser.parse_args()

async def aw_input():  # funcion encargada de esperar la entrada por parte del usuario
    
    try:
        return input('> ')

    except KeyboardInterrupt:
        print('\nCerrando cliente...\n')
        exit(0)

async def main(args):
    
    reader, writer = await a.open_connection(args.host, int(args.port))
    print(f'\nConexión establecida!\n')
    print('SHELL REMOTO'.center(50, '=') + '\n')

    while True:

        try:
            # ya que no se puede hacer await directamente sobre input(), se crea una task encargada de esperar entrada por parte del usuario y se awaitea su resultado
            # para permitir concurrencia mientras se espera el comando
            inp = a.create_task(aw_input())
            
            command = await inp
            writer.write(p.dumps(f'{command}\n'))
            await writer.drain()
            
            if command == 'exit':
                break

            out = b''
                    
            while True:
                    
                packet = await reader.read(4096)
                out += packet

                if sys.getsizeof(packet) <= 4096:
                    break
                
            out = p.loads(out)
            out += '\n' if out[-1::] != '\n' else ''
            print(f'\n{out}')

        except EOFError:
            print('\nLa conexión con el servidor se ha interrumpido repentinamente.\n')      
            exit(0)

    goodbye = p.loads(await reader.read(4096))
    print(f'\n{goodbye}')

if __name__ == '__main__':
    a.run(main(parse_args()))