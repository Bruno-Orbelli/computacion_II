from socket import socket, AF_INET, AF_INET6, SOCK_STREAM, gaierror
from asyncio import Queue, create_task, run, gather, open_connection, StreamReader, StreamWriter, sleep
from pickle import dumps, loads
from sys import getsizeof
from os import getenv
from dotenv import load_dotenv

toSendQueue, toReceiveList = Queue(), []
requestID = 0

async def client_data_loop():
    load_dotenv()
    
    SERVER_PORT = int(getenv("SERVER_PORT"))
    SERVER_IP_ADDRESS = getenv("SERVER_IP_ADDRESS")
    
    # ipv4socket, ipv6socket = socket(AF_INET, SOCK_STREAM), socket(AF_INET6, SOCK_STREAM)
    
    while True:
        if not toSendQueue.empty():
            reader, writer = await open_connection(SERVER_IP_ADDRESS, SERVER_PORT)
            print(await establish_connection_and_execute(reader, writer, SERVER_IP_ADDRESS, SERVER_PORT))

# Agregar opción para usar protocolo UDP? Probar performance de los dos protocolos?
async def establish_connection_and_execute(reader: StreamReader, writer: StreamWriter, ipAddress: str, port: int):   
    '''try:
        ipv4socket.connect((ipAddress, port))
        sock = ipv4socket
    
    # Añadir excepción cuando la dirección o el puerto son incorrectos.

    except gaierror:
        ipv6socket.connect((ipAddress, port))
        ipv6socket
        sock = ipv6socket'''
    
    # En el servidor, añadir algo que indique el final de una respuesta de conversión.
    
    sendTasks = await create_conversion_request_tasks(writer)
    await gather(*sendTasks)

    results = await receive_data(reader)

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

'''async def create_conversion_receive_tasks(reader: StreamReader):
    tasks = [
        create_task(receive_data(reader))
        for _id in toReceiveList
    ]

    return tasks'''

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
    while len(toReceiveList):
        toReceiveRaw = b''
                    
        while True:              
            packet = await reader.read(1024)
            print(packet)
            toReceiveRaw += packet

            if getsizeof(packet) <= 1024:
                break
        
        toReceive = loads(toReceiveRaw)
        toReceiveList.pop()
        print(len(toReceiveList))
        # toReceiveList.remove(toReceive["id"])    
    
    print(toReceive)
    return toReceive

async def main(): # Test main function
    await add_conversion_request("", "", "ps")
    await add_conversion_request("", "", "ps")
    await add_conversion_request("", "", "ps")
    await add_conversion_request("", "", "ps")
    await add_conversion_request("", "", "ps")
    await add_conversion_request("", "", "ps")
    await add_conversion_request("", "", "ps")
    await add_conversion_request("", "", "ps")
    await client_data_loop()

if __name__ == "__main__":
    run(main())

