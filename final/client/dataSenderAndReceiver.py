from asyncio import Queue, QueueEmpty, StreamReader, StreamWriter, TimeoutError, open_connection, wait_for
from pickle import dumps, loads
from sys import path
from os import getenv
from dotenv import load_dotenv
from tqdm import tqdm

try:
    path.index('/home/brunengo/Escritorio/Computación II/computacion_II/final')
except ValueError:
    path.append('/home/brunengo/Escritorio/Computación II/computacion_II/final')

from common.exceptions import InitializationError, ConnectionError

class ClientDataSenderAndReceiver():

    def __init__(self) -> None:
        load_dotenv()
        self.serverIPV4 = ("SERVER_IPV4_ADDRESS", getenv("SERVER_IPV4_ADDRESS"))
        self.serverIPV4Port = ("SERVER_IPV4_PORT", getenv("SERVER_IPV4_PORT"))
        self.serverConnTimeout = ("SERVER_CONNECTION_TIMEOUT", getenv("SERVER_CONNECTION_TIMEOUT"))
        self.clientRetryAttempts = ("CLIENT_RETRY_ATTEMPTS", getenv("CLIENT_RETRY_ATTEMPTS"))

        if None in (self.serverIPV4[1], self.serverIPV4Port[1], self.serverConnTimeout[1], self.clientRetryAttempts[1]):
            envVars = (self.serverIPV4, self.serverIPV4Port, self.serverConnTimeout, self.clientRetryAttempts)
            envVarsStr = ", ".join(envVar[0] for envVar in envVars if envVar[1] is None)
            raise InitializationError(
                f"Could not read environment variable{'s' if tuple(enVar[1] for enVar in envVars).count(None) > 1 else ''} {envVarsStr}. Check for any modifications in '.env'."
                )
        
        self.serverIPV4 = self.serverIPV4[1]
        self.serverIPV4Port = int(self.serverIPV4Port[1])
        self.serverConnTimeout = float(self.serverConnTimeout[1])
        self.clientRetryAttempts = int(self.clientRetryAttempts[1])
        self.retryCounter = self.clientRetryAttempts
        
        self.requestID = 0
        self.toSendQueue = Queue()
        self.toReceiveList = []
    
    async def connect_and_run(self) -> list:       
        try:
            reader, writer = await wait_for(open_connection(self.serverIPV4, self.serverIPV4Port), timeout= self.serverConnTimeout)
        
        except (ConnectionRefusedError, TimeoutError):
            raise ConnectionError(
                f"Failed to connect to conversion server at '{self.serverIPV4}:{self.serverIPV4Port}'. Ensure server is up and running at that address."
                )

        responses = []
        sendProgress = tqdm(desc= "Sending conversion requests...", total= len(self.toReceiveList), colour= "green", position= 0, leave= True)
        receiveProgress = tqdm(desc= "Receiving converted requests...", total= len(self.toReceiveList), colour= "green", position= 1, leave= True)
        
        while not self.toSendQueue.empty() or self.toReceiveList:
            try:
                request = self.toSendQueue.get_nowait()
                await self.send_conversion_request(writer, request, sendProgress)
            
            except QueueEmpty:
                pass

            print("stuck on response")
            await self.get_response_and_append(responses, reader, receiveProgress)
            print("not stuck")
        
        writer.close()
        await writer.wait_closed()
        return responses
    
    async def get_response_and_append(self, responseList: list, reader: StreamReader, receiveProgressBar: tqdm) -> None:
        response = await self.receive_conversion_response(reader, receiveProgressBar)
        if response:
            responseList.append(response)
    
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

    async def send_conversion_request(self, writer: StreamWriter, request: dict, progressBar: tqdm) -> None:
        if not request:
            return
        
        pickledRequest = dumps(request)
        writer.write(pickledRequest)
        await writer.drain()
            
        writer.write(b'\#')
        await writer.drain()
        progressBar.update(1)

        # Agregar una opción de retry?

    async def receive_conversion_response(self, reader: StreamReader, progressBar: tqdm) -> dict:
        try:
            rawResponsePackets = []
            
            while True:
                print("into the loop")
                requestPacket = await reader.read(1024)
                print("read a packet")
                rawResponsePackets.append(requestPacket)
                self.retryCounter = self.clientRetryAttempts
                
                if str(requestPacket)[-3:-1:] == "\#":
                    break
                
            response = loads(b''.join(rawResponsePackets))
            self.toReceiveList.remove(int(response["id"]))
            progressBar.update(1)

        except (TimeoutError, EOFError) as e:
            if isinstance(e, TimeoutError):
                self.retryCounter -= 1
                
                if self.retryCounter == 0:
                    raise ConnectionError(
                        f"No response from conversion server at '{self.serverIPV4}:{self.serverIPV4Port}' after {0.1 * self.clientRetryAttempts}s. Ensure server is still up and try again."
                        )

                return

            else:
                raise ConnectionError(
                    f"Connection with conversion server at '{self.serverIPV4}:{self.serverIPV4Port}' has been lost. Ensure server is still up and try again."
                )
        
        return response

