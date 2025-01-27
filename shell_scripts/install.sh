#!/bin/zsh

cd "$(dirname $(readlink -f "$0"))"
cd ../
sudo rm -rf build
mkdir build && cd build
cmake ..
make
sudo rm -rf /usr/local/include/secure_2pc_package
sudo make install
