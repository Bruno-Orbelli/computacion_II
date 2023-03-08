from asyncio import Queue, create_task, run, gather, open_connection, StreamReader, StreamWriter
from pickle import dumps, loads
from sys import getsizeof
from os import getenv
from dotenv import load_dotenv

class ClientDataSenderAndReceiver():

    def __init__(self) -> None:
        load_dotenv()
        
        self.serverIP = getenv("SERVER_IP_ADDRESS")
        self.serverPort = int(getenv("SERVER_PORT"))
        
        self.requestID = 0
        self.toSendQueue = Queue()
        self.toReceiveList = []

        self.operate = True
    
    async def connect_and_run(self):       
        reader, writer = await open_connection(self.serverIP, self.serverPort)
        
        while self.operate:
            if not self.toSendQueue.empty():
                return await self.exchange_data(reader, writer)
        
        writer.close()
        await writer.wait_closed()

    # Agregar opción para usar protocolo UDP? Probar performance de los dos protocolos?
    async def exchange_data(self, reader: StreamReader, writer: StreamWriter):   
        # En el servidor, añadir algo que indique el final de una respuesta de conversión.
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
        # await toSendQueue.put({convReques})
        await self.toSendQueue.put(data)
        # await toReceiveList.append(requestID)
        self.toReceiveList.append(1)

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
            self.toReceiveList.pop()
            # toReceiveList.remove(toReceive["id"])    
        return responses

async def test_func():
    senderAndReceiver = ClientDataSenderAndReceiver()    
    tasks = [create_task(senderAndReceiver.connect_and_run())]
    tasks.extend([
        create_task(senderAndReceiver.add_conversion_request("", "", "ps"))
        for _ in range(8)
    ])
    result = await gather(*tasks)
    print(result[0])

if __name__ == "__main__":
    run(test_func())

