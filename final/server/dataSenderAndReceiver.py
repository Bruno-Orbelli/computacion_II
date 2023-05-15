from asyncio import StreamReader, StreamWriter, Queue, run, gather, start_server, QueueEmpty, wait_for, TimeoutError
from pickle import dumps, loads
from sys import getsizeof, path
from datetime import datetime
from os import getcwd, getenv
from os.path import dirname
from typing import Literal
from dotenv import load_dotenv

baseDir = dirname(getcwd())
try:
    path.index(baseDir)
except ValueError:
    path.append(baseDir)

from converter import Converter
from logger import ServerLogger
from common.exceptions import InitializationError

class ServerDataSenderAndReceiver():

    def __init__(self) -> None:
        load_dotenv()
        self.serverIPV4Port = ("SERVER_IPV4_PORT", getenv("SERVER_IPV4_PORT"))
        self.serverIPV6Port = ("SERVER_IPV6_PORT", getenv("SERVER_IPV6_PORT"))
        self.logEnabled = ("SERVER_LOG_ENABLED", getenv("SERVER_LOG_ENABLED"))
        self.serverIPProto = ("SERVER_IP_PROTOCOL", getenv("SERVER_IP_PROTOCOL"))

        if None in (self.serverIPV4Port[1], self.serverIPV6Port[1], self.logEnabled[1], self.serverIPProto[1]):
            envVars = (self.serverIPV4Port, self.serverIPV6Port, self.logEnabled, self.serverIPProto)
            envVarsStr = ", ".join(envVar[0] for envVar in envVars if envVar[1] is None)
            raise InitializationError(
                f"Could not read environment variable{'s' if tuple(enVar[1] for enVar in envVars).count(None) > 1 else ''} {envVarsStr}. Check for any modifications in '.env'."
                )
        
        self.serverIPV4Port = int(self.serverIPV4Port[1])
        self.serverIPV6Port = int(self.serverIPV6Port[1])
        self.logEnabled = bool(self.logEnabled[1])
        self.serverIPProto = int(self.serverIPProto[1])
        
        self.clientPrefixTable = {}
        
        self.serverIPV4 = "0.0.0.0"
        self.serverIPV6 = "::"
        
        self.awaitingRequests = Queue()
        self.toSendQueue = Queue()
        self.requestPacketsState = []

        self.logger = ServerLogger()
    
    def get_ip(self) -> str:
        return self.serverIPV4
    
    def get_port(self) -> int:
        return self.serverIPV4Port
       
    async def add_loggable_event_to_queue(self, datetime: datetime, typeOfEntry: str, message: str, context: dict) -> None:
        if self.logEnabled:
            await self.logger.add_event_to_queue((datetime, typeOfEntry, message, context))
    
    async def log_pending_events(self) -> None:
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
        try:
            sizeToRead = int(str(await wait_for(reader.readexactly(32), 0.001))[2:-1:])
            requestPacket = await reader.readexactly(sizeToRead)
            request = loads(requestPacket)
            await self.add_request_to_queue(request, userID)
        
        except TimeoutError:
            pass
            
        return False
    
    async def process_request(self, dbConverter: Converter) -> None:
        processingEvents = await dbConverter.process_requests_in_queue(self.awaitingRequests, self.toSendQueue)
        if processingEvents:
            for event in processingEvents:
                await self.add_loggable_event_to_queue(*event)
    
    async def send_converted_response(self, writer: StreamWriter) -> None:
        try:
            processedRequest = self.toSendQueue.get_nowait()
        except QueueEmpty:
            return
        
        if processedRequest["body"] == "EOT":
            return
        
        serverInternalID = processedRequest["id"]
        processedRequest["id"] = serverInternalID.split("-")[1]
        pickledResponse = dumps(processedRequest)
        
        requestSize = len(pickledResponse)
        sizeToRead = bytes(str(requestSize).zfill(32), 'utf-8')
        writer.write(sizeToRead)
        writer.write(pickledResponse)
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
                await self.send_converted_response(writer)

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
        serverIPv4, serverIPv6 = None, None
        
        if self.serverIPProto in (4, 10):
            while True:
                try:
                    serverIPv4 = await start_server(self.handle_conversion_requests, self.serverIPV4, self.serverIPV4Port)
                    break
                
                except OSError:
                    await self.add_loggable_event_to_queue(datetime.now(), "WARN", "Could not start server: address already in use", {"rejectedAddress": (self.serverIPV4, self.serverIPV4Port)})
                    self.serverIPV4Port += 2
                    await self.add_loggable_event_to_queue(datetime.now(), "INFO", "Retrying server initialization", {"newAddress": (self.serverIPV4, self.serverIPV4Port)})
                    await self.log_pending_events()
                    continue
        
        if self.serverIPProto in (6, 10):
            while True:
                try:
                    serverIPv6 = await start_server(self.handle_conversion_requests, self.serverIPV6, self.serverIPV6Port)
                    break
                
                except OSError:
                    await self.add_loggable_event_to_queue(datetime.now(), "WARN", "Could not start server: address already in use", {"rejectedAddress": (self.serverIPV6, self.serverIPV6Port)})
                    self.serverIPV4Port += 2
                    await self.add_loggable_event_to_queue(datetime.now(), "INFO", "Retrying server initialization", {"newAddress": (self.serverIPV6, self.serverIPV6Port)})
                    await self.log_pending_events()
                    continue
                
        if serverIPv4 and serverIPv6:
            async with serverIPv4, serverIPv6:
                await gather(
                    self.add_loggable_event_to_queue(datetime.now(), "INFO", "Server is up and running", {"serverAddress": (self.serverIPV4, self.serverIPV4Port)}),
                    self.add_loggable_event_to_queue(datetime.now(), "INFO", "Server is up and running", {"serverAddress": (self.serverIPV6, self.serverIPV6Port)}),
                    self.log_pending_events(),
                    serverIPv4.serve_forever(),
                    serverIPv6.serve_forever()
                    )
        
        elif serverIPv4:
            async with serverIPv4:
                await gather(
                    self.add_loggable_event_to_queue(datetime.now(), "INFO", "Server is up and running", {"serverAddress": (self.serverIPV4, self.serverIPV4Port)}),
                    self.log_pending_events(),
                    serverIPv4.serve_forever(),
                    )
        
        else:
            async with serverIPv6:
                await gather(
                    self.add_loggable_event_to_queue(datetime.now(), "INFO", "Server is up and running", {"serverAddress": (self.serverIPV6, self.serverIPV6Port)}),
                    self.log_pending_events(),
                    serverIPv6.serve_forever()
                    )

async def main() -> None:
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

async def on_close() -> None:
    externalLogger = ServerLogger()
    await externalLogger.add_event_to_queue((datetime.now(), "INFO", "Server shutdown with KeyboardInterrupt", {}))
    await externalLogger.log_events() 

if __name__ == "__main__":
    try:
        run(main())
    
    except KeyboardInterrupt as e:
        run(on_close())
        exit(0)
    

