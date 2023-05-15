from asyncio import Queue, QueueEmpty, StreamReader, StreamWriter, TimeoutError, open_connection, wait_for
from pickle import dumps, loads
from sys import path
from os import getcwd, getenv
from os.path import dirname
from typing import Literal
from dotenv import load_dotenv
from tqdm import tqdm

baseDir = dirname(getcwd())
try:
    path.index(baseDir)
except ValueError:
    path.append(baseDir)

from common.exceptions import InitializationError, ConnectionError

class ClientDataSenderAndReceiver():

    def __init__(self) -> None:
        load_dotenv()
        self.serverIPV4 = ("SERVER_IPV4_ADDRESS", getenv("SERVER_IPV4_ADDRESS"))
        self.serverIPV4Port = ("SERVER_IPV4_PORT", getenv("SERVER_IPV4_PORT"))
        self.serverIPV6 = ("SERVER_IPV6_ADDRESS", getenv("SERVER_IPV6_ADDRESS"))
        self.serverIPV6Port = ("SERVER_IPV6_PORT", getenv("SERVER_IPV6_PORT"))
        self.serverConnTimeout = ("SERVER_CONNECTION_TIMEOUT", getenv("SERVER_CONNECTION_TIMEOUT"))
        self.clientRetryAttempts = ("CLIENT_RETRY_ATTEMPTS", getenv("CLIENT_RETRY_ATTEMPTS"))
        self.clientPrefIPProtcol = ("CLIENT_PREFERED_PROTOCOL", getenv("CLIENT_PREFERED_PROTOCOL"))

        if None in (self.serverIPV4[1], self.serverIPV4Port[1], self.serverIPV6[1], self.serverIPV6Port[1], self.serverConnTimeout[1], self.clientRetryAttempts[1], self.clientPrefIPProtcol[1]):
            envVars = (self.serverIPV4, self.serverIPV4Port, self.serverIPV6, self.serverIPV6Port, self.serverConnTimeout, self.clientRetryAttempts, self.clientPrefIPProtcol)
            envVarsStr = ", ".join(envVar[0] for envVar in envVars if envVar[1] is None)
            raise InitializationError(
                f"Could not read environment variable{'s' if tuple(enVar[1] for enVar in envVars).count(None) > 1 else ''} {envVarsStr}. Check for any modifications in '.env'."
                )
        
        self.serverIPV4 = self.serverIPV4[1]
        self.serverIPV4Port = int(self.serverIPV4Port[1])
        self.serverIPV6 = self.serverIPV6[1]
        self.serverIPV6Port = int(self.serverIPV6Port[1])
        self.serverConnTimeout = float(self.serverConnTimeout[1])
        self.clientRetryAttempts = int(self.clientRetryAttempts[1])
        self.clientPrefIPProtcol = int(self.clientPrefIPProtcol[1])
        self.retryCounter = self.clientRetryAttempts
        
        self.requestID = 0
        self.toSendQueue = Queue()
        self.toReceiveList = []
        self.connectedTo = None
    
    async def connect_and_run(self) -> list:       
        ipsAndPorts = [(self.serverIPV4, self.serverIPV4Port), (self.serverIPV6, self.serverIPV6Port)]
        
        if self.clientPrefIPProtcol == 6:
            ipsAndPorts.reverse()
        
        for i, ipWithPort in enumerate(ipsAndPorts):
            try:
                reader, writer = await wait_for(open_connection(ipWithPort[0], ipWithPort[1]), timeout= self.serverConnTimeout)
                self.connectedTo = ipWithPort
                break
                
            except (ConnectionRefusedError, TimeoutError):
                if i == 1:
                    raise ConnectionError(
                        f"Failed to connect to conversion server at both '{self.serverIPV4}:{self.serverIPV4Port}' and '{self.serverIPV6}:{self.serverIPV6Port}'. Ensure server is up and running at one of those addresses."
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

            await self.get_response_and_append(responses, reader, receiveProgress)
        
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
        requestSize = len(pickledRequest)
        sizeToRead = bytes(str(requestSize).zfill(32), 'utf-8')
        
        writer.write(sizeToRead)
        writer.write(pickledRequest)
        await writer.drain()
        
        progressBar.update(1)
    
    async def receive_conversion_response(self, reader: StreamReader, progressBar: tqdm) -> dict:
        try:
            sizeToRead = int(str(await wait_for(reader.readexactly(32), 0.1))[2:-1:])
            requestPacket = await reader.readexactly(sizeToRead)
                
            response = loads(requestPacket)
            self.toReceiveList.remove(int(response["id"]))
            progressBar.update(1)

        except (TimeoutError, EOFError) as e:
            if isinstance(e, TimeoutError):
                self.retryCounter -= 1
                
                if self.retryCounter == 0:
                    raise ConnectionError(
                        f"No response from conversion server at '{self.connectedTo[0]}:{self.connectedTo[1]}' after {0.1 * self.clientRetryAttempts}s. Ensure server is still up and try again."
                        )

                return

            else:
                raise ConnectionError(
                    f"Connection with conversion server at '{self.connectedTo[0]}:{self.connectedTo[1]}' has been lost. Ensure server is still up and try again."
                )
        
        return response

