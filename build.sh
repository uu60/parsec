#!/bin/zsh

cd "$(dirname $(readlink -f "$0"))"
sudo rm -rf build
mkdir build && cd build
cmake .. -G Ninja
ninja
#sudo rm -rf /usr/local/include/secure_2pc_package
#sudo ninja install
