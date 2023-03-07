from asyncio import Queue, create_task, run, gather, open_connection, StreamReader, StreamWriter
from pickle import dumps, loads
from sys import getsizeof
from os import getenv
from dotenv import load_dotenv

toSendQueue, toReceiveList = Queue(), []
requestID = 0

async def data_sender_and_receiver():
    load_dotenv()
    
    SERVER_PORT = int(getenv("SERVER_PORT"))
    SERVER_IP_ADDRESS = getenv("SERVER_IP_ADDRESS")
    
    # ipv4socket, ipv6socket = socket(AF_INET, SOCK_STREAM), socket(AF_INET6, SOCK_STREAM)
    
    if not toSendQueue.empty():
        return await establish_connection_and_execute(SERVER_IP_ADDRESS, SERVER_PORT)

# Agregar opción para usar protocolo UDP? Probar performance de los dos protocolos?
async def establish_connection_and_execute(ipAddress: str, port: int):   
    
    reader, writer = await open_connection(ipAddress, port)
    
    # En el servidor, añadir algo que indique el final de una respuesta de conversión.
    
    sendTasks = await create_conversion_request_tasks(writer)
    await gather(*sendTasks)

    results = await receive_data(reader)
    writer.close()
    await writer.wait_closed()
    return results

async def create_conversion_request_tasks(writer: StreamWriter):
    requests = []
    
    while not toSendQueue.empty():
        requests.append(await toSendQueue.get())
    
    tasks = [
        create_task(send_data(writer, request))
        for request in requests
    ]
    
    return tasks

async def add_conversion_request(originDbType: str, convertTo: str, data):
    global requestID
    requestID += 1
    '''convReques = {
        "id": requestID,
        "originDbType": originDbType,
        "converTo": convertTo,
        "data": data
    }'''
    # await toSendQueue.put({convReques})
    await toSendQueue.put(data)
    # await toReceiveList.append(requestID)
    toReceiveList.append(1)

async def send_data(writer: StreamWriter, data):
    toSend = dumps(data)
    print(toSend)
    
    writer.write(toSend)
    await writer.drain()
    
    writer.write(b'\n')
    await writer.drain()

async def receive_data(reader: StreamReader):
    rawResponse, responses = [], []
    
    while len(toReceiveList):
                    
        while True:              
            packet = await reader.read(1024)
            rawResponse.append(packet)

            if getsizeof(packet) <= 1024:
                break
        
        unpickledResponse = loads(b''.join(pack for pack in rawResponse))
        responses.append(unpickledResponse)
        toReceiveList.pop()
        # toReceiveList.remove(toReceive["id"])    
    
    return responses

async def main(): # Test main function
    await add_conversion_request("", "", "ps")
    await add_conversion_request("", "", "ps")
    await add_conversion_request("", "", "ps")
    await add_conversion_request("", "", "ps")
    await data_sender_and_receiver()

if __name__ == "__main__":
    run(main())

