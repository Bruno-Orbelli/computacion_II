from asyncio import Queue, StreamReader, StreamWriter, Task, TimeoutError, create_task, gather, run, open_connection, wait_for
from pickle import dumps, loads
from sys import getsizeof, path
from os import getenv
from dotenv import load_dotenv

try:
    path.index('/home/brunengo/Escritorio/Computación II/computacion_II/final')
except ValueError:
    path.append('/home/brunengo/Escritorio/Computación II/computacion_II/final')

from common import exceptions

class ClientDataSenderAndReceiver():

    def __init__(self) -> None:
        load_dotenv()
        self.serverIPV4 = ("SERVER_IPV4_ADDRESS", getenv("SERVER_IPV4_ADDRESS"))
        self.serverIPV4Port = ("SERVER_IPV4_PORT", getenv("SERVER_IPV4_PORT"))
        self.serverConnTimeout = ("SERVER_CONNECTION_TIMEOUT", getenv("SERVER_CONNECTION_TIMEOUT"))
        self.serverResponseTimeout = ("SERVER_RESPONSE_TIMEOUT", getenv("SERVER_RESPONSE_TIMEOUT"))

        if None in (self.serverIPV4[1], self.serverIPV4Port[1], self.serverConnTimeout[1], self.serverResponseTimeout[1]):
            envVars = (self.serverIPV4, self.serverIPV4Port, self.serverConnTimeout, self.serverResponseTimeout)
            envVarsStr = ", ".join(envVar[0] for envVar in envVars if envVar[1] is None)
            raise exceptions.InitializationError(
                f"Could not read environment variable{'s' if tuple(enVar[1] for enVar in envVars).count(None) > 1 else ''} {envVarsStr}. Check for any modifications in '.env'."
                )
        
        self.serverIPV4 = self.serverIPV4[1]
        self.serverIPV4Port = int(self.serverIPV4Port[1])
        self.serverConnTimeout = float(self.serverConnTimeout[1])
        self.serverResponseTimeout = float(self.serverResponseTimeout[1])
        
        self.requestID = 0
        self.toSendQueue = Queue()
        self.toReceiveList = []
    
    async def connect_and_run(self) -> list:       
        try:
            reader, writer = await wait_for(open_connection(self.serverIPV4, self.serverIPV4Port), timeout= self.serverConnTimeout)
        
        except (ConnectionRefusedError, TimeoutError, ):
            raise exceptions.ConnectionError(
                f"Failed to connect to conversion server at '{self.serverIPV4}:{self.serverIPV4Port}'. Ensure server is up and running at that address."
                )

        responses = []
        
        while not self.toSendQueue.empty():
            request = await self.toSendQueue.get()
            await self.send_conversion_request(writer, request)
            responses.append(await self.receive_conversion_response(reader))
        
        writer.close()
        await writer.wait_closed()
        return responses

    async def add_conversion_request(self, originDbType: str, convertTo: str, objectType: str, data: 'dict[str, tuple]') -> None:
        self.requestID += 1
        
        convReques = {
            "id": self.requestID,
            "originDbType": originDbType,
            "convertTo": convertTo,
            "objectType": objectType,
            "body": data
        }
        
        await self.toSendQueue.put(convReques)
        self.toReceiveList.append(self.requestID)

    async def send_conversion_request(self, writer: StreamWriter, request: dict) -> None:
        pickledRequest = dumps(request)
        writer.write(pickledRequest)
        await writer.drain()
            
        writer.write(b'\n')
        await writer.drain()
        
        # Agregar una opción de retry?

    async def receive_conversion_response(self, reader: StreamReader) -> dict:
        try:
            rawResponsePackets = []
            
            while True:
                requestPacket = await wait_for(reader.read(1024), self.serverResponseTimeout)
                rawResponsePackets.append(requestPacket)

                if str(requestPacket)[-3:-1:] == "\\n":
                    break
                
            response = loads(b''.join(rawResponsePackets))
            print(response)
            print(self.toReceiveList)
            self.toReceiveList.remove(int(response["id"]))

        except (TimeoutError, EOFError) as e:
            if isinstance(e, TimeoutError):
                raise exceptions.ConnectionError(
                    f"No response from conversion server at '{self.serverIPV4}:{self.serverIPV4Port}' after {self.serverResponseTimeout}s. Ensure server is still up and try again."
                    )

            else:
                raise exceptions.ConnectionError(
                    f"Connection with conversion server at '{self.serverIPV4}:{self.serverIPV4Port}' has been lost. Ensure server is still up and try again."
                )
        
        return response

async def test_func():
    dataSender = ClientDataSenderAndReceiver()
    await dataSender.add_conversion_request("mongo", "sqlite3", None)
    await dataSender.add_conversion_request("sqlite3", "mysql", None)
    await dataSender.add_conversion_request("mysql", "mongo", None)
    await dataSender.add_conversion_request("mongo", "postgresql", None)
    await dataSender.add_conversion_request("postgresql", "sqlite3", None)
    await dataSender.add_conversion_request("mysql", "postgresql", None)
    await dataSender.add_conversion_request("mysql", "mongo", None)
    await dataSender.add_conversion_request("mongo", "sqlite3", None)
    responses = await dataSender.connect_and_run()
    return responses

async def test_func_2():
    dataSender2 = ClientDataSenderAndReceiver()
    await dataSender2.add_conversion_request("mongo", "sqlite3", None)
    await dataSender2.add_conversion_request("sqlite3", "mysql", None)
    await dataSender2.add_conversion_request("mysql", "mongo", None)
    await dataSender2.add_conversion_request("mongo", "postgresql", None)
    await dataSender2.add_conversion_request("postgresql", "sqlite3", None)
    await dataSender2.add_conversion_request("mysql", "postgresql", None)
    await dataSender2.add_conversion_request("mysql", "mongo", None)
    await dataSender2.add_conversion_request("mongo", "sqlite3", None)
    responses = await dataSender2.connect_and_run()
    return responses

async def main():
    responses = await gather(test_func(), test_func_2())
    print(responses)

if __name__ == "__main__":
    run(main())

