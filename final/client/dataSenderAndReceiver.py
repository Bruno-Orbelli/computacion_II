from asyncio import Queue, StreamReader, StreamWriter, Task, TimeoutError, create_task, run, gather, open_connection, wait_for
from pickle import dumps, loads
from sys import getsizeof, path
from os import getenv
from dotenv import load_dotenv

try:
    path.index('/home/brunengo/Escritorio/Computación II/computacion_II/final')
except ValueError:
    path.append('/home/brunengo/Escritorio/Computación II/computacion_II/final')

from common.exceptions import ConnectionError, InitializationError

class ClientDataSenderAndReceiver():

    def __init__(self) -> None:
        load_dotenv()
        self.serverIPV4 = ("SERVER_IPV4_ADDRESS", getenv("SERVER_IPV4_ADDRESS"))
        self.serverIPV4Port = ("SERVER_IPV4_PORT", getenv("SERVER_IPV4_PORT"))
        self.serverConnTimeout = ("SERVER_CONNECTION_TIMEOUT", getenv("SERVER_CONNECTION_TIMEOUT"))
        self.serverResponseTimeout = ("SERVER_RESPONSE_TIMEOUT", getenv("SERVER_RESPONSE_TIMEOUT"))

        if None in (self.serverIP[1], self.serverPort[1], self.serverConnTimeout[1], self.serverResponseTimeout[1]):
            envVars = (self.serverIP, self.serverPort, self.serverConnTimeout, self.serverResponseTimeout)
            envVarsStr = ", ".join(envVar[0] for envVar in envVars if envVar[1] is None)
            raise InitializationError(
                f"Could not read environment variable{'s' if tuple(enVar[1] for enVar in envVars).count(None) > 1 else ''} {envVarsStr}. Check for any modifications in '.env'."
                )
        
        self.serverIP = self.serverIP[1]
        self.serverPort = int(self.serverPort[1])
        self.serverConnTimeout = float(self.serverConnTimeout[1])
        self.serverResponseTimeout = float(self.serverResponseTimeout[1])
        
        self.requestID = 0
        self.toSendQueue = Queue()
        self.toReceiveList = []
    
    async def connect_and_run(self) -> list:       
        try:
            reader, writer = await wait_for(open_connection(self.serverIP, self.serverPort), timeout= self.serverConnTimeout)
        
        except (ConnectionRefusedError, TimeoutError):
            raise ConnectionError(
                f"Failed to connect to conversion server at '{self.serverIP}:{self.serverPort}'. Ensure server is up and running at that address."
                )

        while True:
            if not self.toSendQueue.empty():
                result = await self.exchange_data(reader, writer)
                writer.close()
                await writer.wait_closed()
                return result

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
    senderAndReceiver = ClientDataSenderAndReceiver()    
    tasks = [create_task(senderAndReceiver.connect_and_run())]
    tasks.extend([
        create_task(senderAndReceiver.add_conversion_request("", "", "ps")),
        create_task(senderAndReceiver.add_conversion_request("", "", "ls")),
        create_task(senderAndReceiver.add_conversion_request("", "", "cat /")),
        create_task(senderAndReceiver.add_conversion_request("", "", "ps")),
        create_task(senderAndReceiver.add_conversion_request("", "", "ls")),
        create_task(senderAndReceiver.add_conversion_request("", "", "cat /")),
        create_task(senderAndReceiver.add_conversion_request("", "", "ps"))
    ])
    result = await gather(*tasks)
    print(result[0])

if __name__ == "__main__":
    run(test_func())

