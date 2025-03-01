#!/bin/zsh

sh ../shell_scripts/install.sh
cd "$(dirname $(readlink -f "$0"))"
sudo rm -rf build
mkdir build && cd build
cmake .. -G Ninja
ninja
