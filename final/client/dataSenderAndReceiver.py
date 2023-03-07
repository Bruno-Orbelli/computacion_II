from socket import socket, AF_INET, AF_INET6, SOCK_STREAM, SOCK_NONBLOCK, gaierror
from asyncio import Queue, create_task, run
from pickle import dumps, loads
from sys import getsizeof
from os import getenv
from dotenv import load_dotenv

toSendQueue, toReceiveList = Queue(), []
requestID = 0

# Agregar opción para usar protocolo UDP? Probar performance de los dos protocolos?
async def establish_connection_and_execute():
    load_dotenv()
    
    SERVER_PORT = int(getenv("SERVER_PORT"))
    SERVER_IP_ADDRESS = getenv("SERVER_IP_ADDRESS")
    
    ipv4socket, ipv6socket = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK), socket(AF_INET6, SOCK_STREAM | SOCK_NONBLOCK)
    
    try:
        ipv4socket.connect((SERVER_IP_ADDRESS, SERVER_PORT))
        sock = ipv4socket
    
    # Añadir excepción cuando la dirección o el puerto son incorrectos.

    except gaierror:
        ipv6socket.connect((SERVER_IP_ADDRESS, SERVER_PORT))
        sock = ipv6socket
    
    # En el servidor, añadir algo que indique el final de una respuesta de conversión.
    
    tasks = [
        create_task(send_data(sock, request))
        for request in iter(toSendQueue.get, None)
    ]

    tasks.extend([
        create_task(receive_data)
    ])

    while True:
        if not toSendQueue.empty():
            convRequest = toSendQueue.get()
        run(send_data(sock, convRequest))

async def add_conversion_request(originDbType: str, convertTo: str, data):
    requestID += 1
    convReques = {
        "id": requestID,
        "originDbType": originDbType,
        "converTo": convertTo,
        "data": data
    }
    await toSendQueue.put({convReques})
    await toReceiveList.append(requestID)

async def send_data(sock: socket, data):
    toSend = dumps((data))
    await sock.sendall(toSend)

async def receive_data(sock: socket):
    toReceiveRaw = b''
                
    while True:              
        packet = await sock.recv(1024)
        toReceiveRaw += packet

        if getsizeof(packet) <= 1024:
            break

    toReceive = loads(toReceiveRaw)
    toReceiveList.remove(toReceive["id"])
    return toReceive

if __name__ == "__main__":
    sock = establish_connection_and_get_socket()
    # run(send_data(sock, "ps"))

