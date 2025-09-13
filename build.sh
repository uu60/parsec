#!/bin/sh

# Parse command line arguments
USE_ASAN=false
while [ "$#" -gt 0 ]; do
  case "$1" in
    --asan)
      USE_ASAN=true
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [--asan] [--help]"
      echo "  --asan    Enable AddressSanitizer build"
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

# cd to script dir (portable; avoids 'readlink -f' on macOS)
SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd -P) || exit 1
cd "$SCRIPT_DIR" || exit 1

sudo rm -rf build
mkdir build && cd build || exit 1

# Configure CMake with or without ASAN
if [ "$USE_ASAN" = true ]; then
  echo "Building with AddressSanitizer enabled (O1)…"
  cmake .. -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_C_FLAGS_RELWITHDEBINFO="-O1 -g -fno-omit-frame-pointer -fsanitize=address" \
    -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O1 -g -fno-omit-frame-pointer -fsanitize=address" \
    -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address" \
    -DCMAKE_SHARED_LINKER_FLAGS="-fsanitize=address" \
    -DCMAKE_MODULE_LINKER_FLAGS="-fsanitize=address"
else
  echo "Building without AddressSanitizer (O2)…"
  cmake .. -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_FLAGS_RELEASE="-O2 -DNDEBUG" \
    -DCMAKE_CXX_FLAGS_RELEASE="-O2 -DNDEBUG"
fi

ninja
# sudo ninja install