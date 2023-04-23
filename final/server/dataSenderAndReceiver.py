from asyncio import StreamReader, StreamWriter, Queue, run, gather, start_server
from pickle import dumps, loads
from sys import getsizeof, path
from datetime import datetime
from os import getenv
from dotenv import load_dotenv

try:
    path.index('/home/brunengo/Escritorio/Computación II/computacion_II/final')
except ValueError:
    path.append('/home/brunengo/Escritorio/Computación II/computacion_II/final')

from converter import Converter
from logger import ServerLogger
from common.exceptions import InitializationError

class ServerDataSenderAndReceiver():

    def __init__(self, daemon = False, logEnabled = True) -> None:
        load_dotenv()
        self.serverIPV4 = ("SERVER_IPV4_ADDRESS", getenv("SERVER_IPV4_ADDRESS"))
        self.serverIPV4Port = ("SERVER_IPV4_PORT", getenv("SERVER_IPV4_PORT"))

        if None in (self.serverIPV4[1], self.serverIPV4Port[1]):
            envVars = (self.serverIPV4, self.serverIPV4Port)
            envVarsStr = ", ".join(envVar[0] for envVar in envVars if envVar[1] is None)
            raise InitializationError(
                f"Could not read environment variable{'s' if tuple(enVar[1] for enVar in envVars).count(None) > 1 else ''} {envVarsStr}. Check for any modifications in '.env'."
                )
        
        self.serverIPV4 = self.serverIPV4[1]
        self.serverIPV4Port = int(self.serverIPV4Port[1])
        
        self.clientPrefixTable = {}
        self.daemon = daemon  # Flag especificada en el argparse del init.py principal
        self.logEnabled = logEnabled  # Flag especificada en el argparse del init.py principal
        
        self.awaitingRequests = Queue()
        self.toSendQueue = Queue()

        self.logger = ServerLogger()
    
    def get_ip(self) -> str:
        return self.serverIPV4
    
    def get_port(self) -> int:
        return self.serverIPV4Port
    
    async def add_loggable_event_to_queue(self, datetime: datetime, typeOfEntry: str, message: str, context: dict):
        await self.logger.add_event_to_queue((datetime, typeOfEntry, message, context))
    
    async def log_pending_events(self):
        await self.logger.log_events()
    
    async def get_prefix_from_table(self, clientIP: str) -> str:
        if not self.clientPrefixTable:
            self.clientPrefixTable.update({clientIP: 1})
            await self.add_loggable_event_to_queue(datetime.now(), "INFO", "Prefix assigned to client IP", {"clientIP": clientIP, "prefix": 1})
            return 1
        
        elif clientIP in self.clientPrefixTable:
            return self.clientPrefixTable[clientIP]
        
        else:
            highestPrefixAssigned = max([prefix for prefix in self.clientPrefixTable.values()])
            
            for num in range(1, highestPrefixAssigned):
                if num not in self.clientPrefixTable.values():
                    self.clientPrefixTable.update({clientIP: str(num)})
                    await self.add_loggable_event_to_queue(datetime.now(), "INFO", "Prefix assigned to client IP", {"clientIP": clientIP, "prefix": num})
                    return str(num)
            
            else:
                self.clientPrefixTable.update({clientIP: str(highestPrefixAssigned + 1)})
                await self.add_loggable_event_to_queue(datetime.now(), "INFO", "Prefix assigned to client IP", {"clientIP": clientIP, "prefix": highestPrefixAssigned + 1})
                return str(highestPrefixAssigned + 1)
    
    def remove_prefix_from_table(self, clientIP: str) -> None:
        self.clientPrefixTable.pop(clientIP)
    
    async def add_request_to_queue(self, request: 'dict[str, str | int]', userID: str) -> None:      
        request["id"] = f"{userID}-{request['id']}"
        if request["id"] != "None-None":
            await self.add_loggable_event_to_queue(datetime.now(), "INFO", "Succesfully received request", {
                "requestID": request["id"], 
                "requestSize": f"{getsizeof(str(request))}B", 
                "originDbType": request["originDbType"],
                "convertTo": request["convertTo"]
                })
        
        await self.awaitingRequests.put(request)
    
    async def read_conversion_request(self, reader: StreamReader, userID: str) -> bool:
        rawRequestPackets = []
            
        while True:
            requestPacket = await reader.read(1024)
            rawRequestPackets.append(requestPacket)
            
            if str(requestPacket) == "b''":
                await self.add_request_to_queue(
                    {"id": None,
                     "originDbType": None,
                     "convertTo": None,
                     "objectType": None,
                     "body": "EOT"
                    },
                    None
                )
                return True

            elif str(requestPacket)[-3:-1:] == "\\n":
                break
                
        request = loads(b''.join(rawRequestPackets))
        await self.add_request_to_queue(request, userID)
        
        return False
    
    async def process_request(self, dbConverter: Converter) -> None:
        processingEvents = await dbConverter.process_requests_in_queue(self.awaitingRequests, self.toSendQueue)
        if processingEvents:
            for event in processingEvents:
                await self.add_loggable_event_to_queue(*event)
    
    async def send_converted_response(self, writer: StreamWriter, userID: str) -> None:
        processedRequest = await self.toSendQueue.get()

        if processedRequest["body"] == "EOT":
            return
        
        serverInternalID = processedRequest["id"]
        processedRequest["id"] = serverInternalID.split("-")[1]
        pickledResponse = dumps(processedRequest)
        
        writer.write(pickledResponse)
        await writer.drain()
        writer.write(b"\n")
        await writer.drain()

        await self.add_loggable_event_to_queue(datetime.now(), "INFO", "Sucesfully sent request response", {"requestID": serverInternalID, "responseSize": f"{getsizeof(str(processedRequest))}B"})
    
    async def handle_conversion_requests(self, reader: StreamReader, writer: StreamWriter) -> None:
        clientAddress = writer.get_extra_info("peername")
        clientPrefix = await self.get_prefix_from_table(clientAddress[0])
        userID = f"{clientPrefix}{clientAddress[1]}"
        await self.add_loggable_event_to_queue(datetime.now(), "INFO", "User sucesfully connected to server", {"userID": userID})
        
        dbConverter = Converter()
        endOfTransmission = False
        
        while not endOfTransmission:            
            try:
                endOfTransmission = await self.read_conversion_request(reader, userID)
                await self.process_request(dbConverter)
                await self.send_converted_response(writer, userID)

            except (EOFError, ConnectionResetError):
                await self.add_loggable_event_to_queue(datetime.now(), "ERR", "Unexpectedly lost connection with user", {"userID": userID})
                # dbConverter.remove_zombie_requests(self.clientPrefixTable[clientAddress[0]] + clientAddress[1])
                self.remove_prefix_from_table(clientAddress[0])
                break
        
        else:
            await self.add_loggable_event_to_queue(datetime.now(), "INFO", "User has ended connection with server", {"userID": userID})
    
        await self.log_pending_events()
        return
    
    async def start_and_serve(self) -> None:       
        while True:
            try:
                server = await start_server(self.handle_conversion_requests, self.serverIPV4, self.serverIPV4Port)
            
            except OSError:
                await self.add_loggable_event_to_queue(datetime.now(), "WARN", "Could not start server: address already in use", {"rejectedAddress": (self.serverIPV4, self.serverIPV4Port)})
                self.serverIPV4Port += 2
                await self.add_loggable_event_to_queue(datetime.now(), "INFO", "Retrying server initialization", {"newAddress": (self.serverIPV4, self.serverIPV4Port)})
                await self.log_pending_events()
                continue
            
            async with server:
                await gather(
                    self.add_loggable_event_to_queue(datetime.now(), "INFO", "Server is up and running", {"serverAddress": (self.serverIPV4, self.serverIPV4Port)}),
                    self.log_pending_events(),
                    server.serve_forever()
                    )

async def main():
    try:
        server = ServerDataSenderAndReceiver()
        await server.start_and_serve()
    
    except Exception as e:
        externalLogger = ServerLogger()
        
        if isinstance(e, InitializationError):
            await externalLogger.add_event_to_queue((datetime.now(), "FATAL", "A configuration error has prevented server from starting", {"excpType": type(e), "excpMsg": e}))
            await externalLogger.log_events()
        
        else:
            await externalLogger.add_event_to_queue((datetime.now(), "FATAL", "An unexpected error has forced server to shutdown", {"excpType": type(e), "excpMsg": e}))
            await externalLogger.log_events()

        exit(1)

async def on_close(e: 'Exception'):
    externalLogger = ServerLogger()
    await externalLogger.add_event_to_queue((datetime.now(), "INFO", "Server shutdown with KeyboardInterrupt", {}))
    await externalLogger.log_events() 

if __name__ == "__main__":
    try:
        run(main())
    
    except KeyboardInterrupt as e:
        run(on_close(e))
        exit(0)
    

