#!/bin/bash

echo "Installing docker and docker-compose..."
apt-get install -y docker docker-compose
cd docker/
echo "Building server..."
docker-compose up 