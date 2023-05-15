#!/bin/bash

echo "Installing docker..."
sudo apt-get install -y docker
echo "Building image..."
if [[ "$(sudo docker images -q conversql_app:latest 2> /dev/null)" == "" ]]; then
    echo "Image conversql_app:latest already built."
else
    sudo docker build . --file appDockerfile --build-arg USER_ID=$(id -u) --build-arg GROUP_ID=$(id -g) --tag conversql_app:latest
fi
echo "Running app..."
sudo docker run -it --rm --net=host -v /:/volumebind conversql_app
