#!/bin/zsh

# Parse command line arguments
USE_ASAN=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --asan)
            USE_ASAN=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--asan] [--help]"
            echo "  --asan    Enable AddressSanitizer for memory leak detection"
            echo "  --help    Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

cd "$(dirname $(readlink -f "$0"))"
sudo rm -rf build
mkdir build && cd build

# Configure cmake with or without ASAN
if [ "$USE_ASAN" = true ]; then
    echo "Building with AddressSanitizer enabled..."
    cmake .. -G Ninja -DENABLE_ASAN=ON
else
    echo "Building without AddressSanitizer..."
    cmake .. -G Ninja
fi

ninja
#sudo rm -rf /usr/local/include/parsec
#sudo ninja install
