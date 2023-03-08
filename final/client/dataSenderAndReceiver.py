from asyncio import Queue, create_task, run, gather, open_connection, wait_for, TimeoutError, StreamReader, StreamWriter
from pickle import dumps, loads
from sys import getsizeof
from os import getenv
from dotenv import load_dotenv

from exceptions import ConnectionError, InitializationError

class ClientDataSenderAndReceiver():

    def __init__(self) -> None:
        load_dotenv()
        
        self.serverIP = ("SERVER_IP_ADDRESS", getenv("SERVER_IP_ADRESS"))
        self.serverPort = ("SERVER_PORT", getenv("SERVER_POT"))
        self.serverConnTimeout = ("SERVER_CONNECTION_TIMEOUT", getenv("SERVER_CONNECTION_TIMEOUT"))

        if None in (self.serverIP[1], self.serverPort[1], self.serverConnTimeout[1]):
            envVars = (self.serverIP, self.serverPort, self.serverConnTimeout)
            envVarsStr = ", ".join(envVar[0] for envVar in envVars if envVar[1] is None)
            raise InitializationError(
                f"Could not read environment variable{'s' if tuple(enVar[1] for enVar in envVars).count(None) > 1 else ''} {envVarsStr}. Check for any modifications in '.env'."
                )
        
        self.serverPort = int(self.serverPort)
        self.serverConnTimeout = float(self.serverConnTimeout)
        
        self.requestID = 0
        self.toSendQueue = Queue()
        self.toReceiveList = []
    
    async def connect_and_run(self):       
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

    async def exchange_data(self, reader: StreamReader, writer: StreamWriter):   
        # En el servidor, añadir algo que indique el final de una respuesta de conversión (\n).
        infoSendingTasks = await self.create_conversion_request_tasks(writer)
        await gather(*infoSendingTasks)

        results = await self.receive_data(reader)
        return results

    async def create_conversion_request_tasks(self, writer: StreamWriter):
        requests = []
        
        while not self.toSendQueue.empty():
            requests.append(await self.toSendQueue.get())
        
        tasks = [
            create_task(self.send_data(writer, request))
            for request in requests
        ]
        
        return tasks

    async def add_conversion_request(self, originDbType: str, convertTo: str, data):
        self.requestID += 1
        
        convReques = {
            "id": self.requestID,
            "originDbType": originDbType,
            "converTo": convertTo,
            "data": data
        }
        
        await self.toSendQueue.put(convReques)
        self.toReceiveList.append(self.requestID)

    async def send_data(self, writer: StreamWriter, data):
        toSend = dumps(data)
        writer.write(toSend)
        await writer.drain()
        
        writer.write(b'\n')
        await writer.drain()

    async def receive_data(self, reader: StreamReader):
        responses = []

        while len(self.toReceiveList):
            rawResponse = []
            packet = b''

            while getsizeof(packet) > 1024 or not packet:              
                packet = await reader.read(1024)
                rawResponse.append(packet)
            
            unpickledResponse = loads(b''.join(pack for pack in rawResponse))
            responses.append(unpickledResponse)
            self.toReceiveList.remove(unpickledResponse["id"])    
        
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

