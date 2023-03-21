from asyncio import Queue, StreamReader, StreamWriter, Task, create_task, run, gather, open_connection, wait_for, start_server
from pickle import dumps, loads
from sys import getsizeof, path
from os import getenv
from dotenv import load_dotenv

try:
    path.index('/home/brunengo/Escritorio/Computación II/computacion_II/final')
except ValueError:
    path.append('/home/brunengo/Escritorio/Computación II/computacion_II/final')

from common.exceptions import ConnectionError, InitializationError

class ServerDataSenderAndReceiver():

    def __init__(self, daemon = False) -> None:
        load_dotenv()
        self.serverIP = ("SERVER_IPV4_ADDRESS", getenv("SERVER_IPV4_ADDRESS"))
        self.serverPort = ("SERVER_IPV4_PORT", getenv("SERVER_IPV4_PORT"))

        if None in (self.serverIP[1], self.serverPort[1]):
            envVars = (self.serverIP, self.serverPort)
            envVarsStr = ", ".join(envVar[0] for envVar in envVars if envVar[1] is None)
            raise InitializationError(
                f"Could not read environment variable{'s' if tuple(enVar[1] for enVar in envVars).count(None) > 1 else ''} {envVarsStr}. Check for any modifications in '.env'."
                )
        
        self.serverIP = self.serverIP[1]
        self.serverPort = int(self.serverPort[1])
        self.daemon = daemon  # Flag especificada en el argparse del init.py principal
        
        self.toSendQueue = Queue()
    
    async def handle_conversion_request(self, reader: StreamReader, writer: StreamWriter):
        clientAdress = writer.get_extra_info("peername")  # A ser usado en el log
        
        while True:
            data = await reader.readline()
            print(data)  
            
            try:
                data = loads(data)
                        
                # Server conversion code...
                    
            except (EOFError, ConnectionResetError):
                print(f'Conexión finalizada inesperadamente con {None}.\n')
                return
    
    async def start_and_serve(self) -> list:       
        while True:
            try:
                server = await start_server(self.handle_conversion_request, self.serverIP, self.serverPort)
            
            except OSError:
                self.serverPort += 2
                continue
            
            async with server:
                try:
                    await server.serve_forever()
                    break
                
                except: # Cachear posibles errores
                    print("error")
                    exit(1)

    async def exchange_data(self, reader: StreamReader, writer: StreamWriter) -> list:   
        # En el servidor, añadir algo que indique el final de una respuesta de conversión (\n).
        infoSendingTasks = await self.create_conversion_request_tasks(writer)
        try:
            await gather(*infoSendingTasks)
        
        except ConnectionResetError:
            raise ConnectionError(
                f"Connection with conversion server at '{self.serverIP}:{self.serverPort}' has been lost. Ensure server is still up and try again."
                )

        results = await self.receive_data(reader)
        return results

    async def create_conversion_request_tasks(self, writer: StreamWriter) -> 'list[Task[None]]':
        requests = []
        
        while not self.toSendQueue.empty():
            requests.append(await self.toSendQueue.get())
        
        tasks = [
            create_task(self.send_data(writer, request))
            for request in requests
        ]
        
        return tasks

    async def add_conversion_request(self, originDbType: str, convertTo: str, data) -> None:
        self.requestID += 1
        
        convReques = {
            "id": self.requestID,
            "originDbType": originDbType,
            "converTo": convertTo,
            "data": data
        }
        
        await self.toSendQueue.put(convReques["data"])
        self.toReceiveList.append(self.requestID)

    async def send_data(self, writer: StreamWriter, data) -> None:
        toSend = dumps(data)
        writer.write(toSend)
        await writer.drain()
            
        writer.write(b'\n')
        await writer.drain()
        
        # Agregar una opción de retry?

    async def receive_data(self, reader: StreamReader) -> list:
        responses = []

        while len(self.toReceiveList):
            rawResponse = []
            packet = b''

            try:
                while getsizeof(packet) > 1024 or not packet:              
                    packet = await wait_for(reader.read(1024), self.serverResponseTimeout)
                    rawResponse.append(packet)
            
            except TimeoutError:
                raise ConnectionError(
                    f"No response from conversion server at '{self.serverIP}:{self.serverPort}' after {self.serverResponseTimeout}s. Ensure server is still up and try again."
                    ) 
            
            unpickledResponse = loads(b''.join(pack for pack in rawResponse))
            responses.append(unpickledResponse)
            self.toReceiveList.pop()
            # self.toReceiveList.remove(unpickledResponse["id"])    
        
        return responses

async def test_func():
    server = ServerDataSenderAndReceiver()   
    await server.start_and_serve()

if __name__ == "__main__":
    run(test_func())
    

