from asyncio import StreamReader, StreamWriter, Queue, create_task, run, gather, open_connection, wait_for, start_server
from pickle import dumps, loads
from sys import getsizeof, path
from os import getenv
from dotenv import load_dotenv

try:
    path.index('/home/brunengo/Escritorio/Computación II/computacion_II/final')
except ValueError:
    path.append('/home/brunengo/Escritorio/Computación II/computacion_II/final')

from converter import Converter
from common.exceptions import InitializationError

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
        
        self.clientPrefixTable = {}
        self.daemon = daemon  # Flag especificada en el argparse del init.py principal
        
        self.awaitingRequests = Queue()
        self.toSendQueue = Queue()
    
    def get_prefix_from_table(self, clientIP: str) -> str:
        if not self.clientPrefixTable:
            self.clientPrefixTable.update({clientIP: 1})
            return 1
        
        elif clientIP in self.clientPrefixTable:
            return self.clientPrefixTable[clientIP]
        
        else:
            highestPrefixAssigned = max([prefix for prefix in self.clientPrefixTable.values()])
            
            for num in range(1, highestPrefixAssigned):
                if num not in self.clientPrefixTable.values():
                    self.clientPrefixTable.update({clientIP: str(num)})
                    return str(num)
            
            else:
                self.clientPrefixTable.update({clientIP: str(highestPrefixAssigned + 1)})
                return str(highestPrefixAssigned + 1)
    
    def remove_prefix_from_table(self, clientIP: str) -> None:
        self.clientPrefixTable.pop(clientIP)
    
    async def add_request_to_queue(self, request: 'dict[str, str | int]', clientAddress: tuple) -> None:
        clientPrefix = self.get_prefix_from_table(clientAddress[0])
        request["id"] = f"{clientPrefix}{clientAddress[1]}-{request['id']}"
        await self.awaitingRequests.put(request)
    
    async def read_conversion_request(self, reader: StreamReader, clientAddress: str) -> bool:
        rawRequestPackets = []
            
        while True:
            requestPacket = await reader.read(1024)
            rawRequestPackets.append(requestPacket)
            
            if str(requestPacket)[-5:-1:] == "\\n\\n":
                return True

            elif str(requestPacket)[-3:-1:] == "\\n":
                break
                
        request = loads(b''.join(rawRequestPackets)) 
        await self.add_request_to_queue(request, clientAddress)
        
        return False
    
    async def send_converted_response(self, writer: StreamWriter):
        processedRequest = await self.toSendQueue.get()
        processedRequest["id"] = processedRequest["id"].split("-")[1]
        pickledResponse = dumps(processedRequest)
        
        writer.write(pickledResponse)
        await writer.drain()
        writer.write(b"\n")
        await writer.drain()
    
    async def handle_conversion_requests(self, reader: StreamReader, writer: StreamWriter) -> None:
        clientAddress = writer.get_extra_info("peername")  # A ser usado en el log
        dbConverter = Converter()
        endOfTransmission = False

        while not endOfTransmission:            
            try:
                endOfTransmission = await self.read_conversion_request(reader, clientAddress)
                await dbConverter.process_requests_in_queue(self.awaitingRequests, self.toSendQueue)
                await self.send_converted_response(writer)
                    
            except (EOFError, ConnectionResetError):
                dbConverter.remove_zombie_requests(self.clientPrefixTable[clientAddress[0]] + clientAddress[1])
                self.remove_prefix_from_table(clientAddress[0])
                return
        
        return
    
    async def start_and_serve(self) -> None:       
        while True:
            try:
                server = await start_server(self.handle_conversion_requests, self.serverIP, self.serverPort)
            
            except OSError:
                self.serverPort += 2
                continue
            
            async with server:
                try:
                    await server.serve_forever()
                    break
                
                except Exception as e: # Cachear posibles errores
                    print(e)
                    exit(1)

async def test_func():
    server = ServerDataSenderAndReceiver()   
    await server.start_and_serve()

if __name__ == "__main__":
    run(test_func())
    

