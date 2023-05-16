## Installing ConverSQL

### Server machine
1. Clone this [repository](https://github.com/Bruno-Orbelli/computacion_II.git) or download as a .ZIP file and extract.
2. Inside */your/repository/path/final*, run the following command: `sudo ./compose_server.sh`.
3. Done! Server will be up and running.

### Client machine
1. Clone the repository or download as .ZIP and extract.
2. Inside */your/repository/path/final*, run the following command: `./launch.sh`.
3. Done! Client will be up and running.

**WARN**: If your server and client machine are different, you'll need to tweak both final/.env and final/docker/dockerenv.txt (both env files have to have matching configuration), as, by default, ConverSQL assumes the server is hosted in the same computer as the client. Changing `SERVER_IPV4_ADDRESS` and `SERVER_IPV6_ADDRESS` to point at server host's IP4 and IP6 is usually enough. 