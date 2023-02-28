from asyncio import sslproto, subprocess
import socketserver as ss
import socket
import argparse
from subprocess import Popen, PIPE
import subprocess
import threading


class ForkedTCPServer4(ss.ForkingMixIn, ss.TCPServer):
    address_family = socket.AF_INET
    pass

class ForkedTCPServer6(ss.ForkingMixIn, ss.TCPServer):
    address_family = socket.AF_INET6
    pass


class ThreadedTCPServer4(ss.ThreadingMixIn, ss.TCPServer):
    address_family = socket.AF_INET
    pass

class ThreadedTCPServer6(ss.ThreadingMixIn, ss.TCPServer):
    address_family = socket.AF_INET6
    pass

class TCPRequestHandler(ss.BaseRequestHandler):

    def handle(self):

        FORMAT = 'utf-8'
        HEADER = 64
        DISCONNECT_MESSAGE = '!DISCONNECT'
        print(f'[NEW CONNECTION] {self.client_address} connected.')

        while True:
            msg_len = self.request.recv(HEADER).decode(FORMAT)
            if msg_len:
                msg_len = int(msg_len)
                command = self.request.recv(msg_len).decode(FORMAT)
                if command == DISCONNECT_MESSAGE or command == 'exit':
                    print(DISCONNECT_MESSAGE)
                    break
                
                print(f'[{self.client_address}] > Command {command} Executed')
                output = 'output: \n'
                output += subprocess.getoutput(command)
                self.request.send(output.encode(FORMAT))

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=0, help='puerto del servidor')
    parser.add_argument('-c', '--conc', required=True, choices=['p', 't'], help='tipo de concurrencia: multi-process (p), multi-threads (t)')
    args = parser.parse_args()

    return args

def server_p(d):
    ss.TCPServer.allow_reuse_address = True
    if d[0] == socket.AF_INET:
        with ForkedTCPServer4(d[4], TCPRequestHandler) as server:
            server.serve_forever()
    
    elif d[0] == socket.AF_INET6:
        with ForkedTCPServer6(d[4], TCPRequestHandler) as server:
                server.serve_forever()


def server_t(d):
    ss.TCPServer.allow_reuse_address = True
    if d[0] == socket.AF_INET:
        with ThreadedTCPServer4(d[4], TCPRequestHandler) as server:
            server.serve_forever()
    
    elif d[0] == socket.AF_INET6:
        with ThreadedTCPServer6(d[4], TCPRequestHandler) as server:
                server.serve_forever()

def main(args):

    options = {'t': ss.ThreadingTCPServer, 'p': ss.ForkingTCPServer}
    addrs = []
    addrs.append(socket.getaddrinfo("localhost", args.port, socket.AF_INET, 1)[0])
    addrs.append(socket.getaddrinfo("localhost", args.port, socket.AF_INET6, 1)[0])
    
    if args.conc == 'p':
        for d in addrs:
            print(f'[WAITING] Process Server is waiting for connections on {d[4]}')
            threading.Thread(target=server_p, args=(d,)).start()

    elif args.conc == 't':
        for d in addrs:
            print(f'[WAITING] Threading Server is waiting for connections on {d[4]}')
            threading.Thread(target=server_t, args=(d,)).start()

if __name__ == "__main__":
    main(args())