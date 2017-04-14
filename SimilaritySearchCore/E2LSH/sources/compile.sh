#!/bin/bash
clear
make clean
make
echo "copying file to ../../lshLibs "
sudo cp libLSH.so ../../lshLibs/

