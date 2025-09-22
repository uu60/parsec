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

# Check if libsqlparser.so exists in root directory
echo "Checking for libsqlparser.so in root directory..."
if [ ! -f "libsqlparser.so" ]; then
    echo "libsqlparser.so not found in root directory. Building sqlparser..."

    # Navigate to sqlparser directory
    cd db/third_party/sql-parser || exit 1

    # Clean previous build and compile sqlparser
    echo "Cleaning previous sqlparser build..."
    make clean

    echo "Compiling sqlparser..."
    make library

    # Check if compilation was successful
    if [ ! -f "libsqlparser.so" ]; then
        echo "Error: Failed to compile libsqlparser.so"
        exit 1
    fi

    # Move the library to project root
    echo "Moving libsqlparser.so to project root..."
    mv libsqlparser.so "$SCRIPT_DIR/" || exit 1

    # Return to script directory
    cd "$SCRIPT_DIR" || exit 1

    echo "libsqlparser.so successfully built and moved to root directory."
else
    echo "libsqlparser.so already exists in root directory."
fi

# cd to script dir (portable; avoids 'readlink -f' on macOS)
SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd -P) || exit 1
cd "$SCRIPT_DIR" || exit 1

rm -rf build
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

# Print actual CMake configuration after cmake runs
echo ""
echo "=== Actual CMake Configuration ==="
if [ -f "CMakeCache.txt" ]; then
  echo "Build Type: $(grep "CMAKE_BUILD_TYPE:" CMakeCache.txt | cut -d'=' -f2)"
  echo "C Compiler: $(grep "CMAKE_C_COMPILER:" CMakeCache.txt | cut -d'=' -f2)"
  echo "CXX Compiler: $(grep "CMAKE_CXX_COMPILER:" CMakeCache.txt | cut -d'=' -f2)"
  
  # Extract actual flags being used
  echo ""
  echo "Actual Compiler Flags:"
  if [ "$USE_ASAN" = true ]; then
    echo "C Flags (RelWithDebInfo): $(grep "CMAKE_C_FLAGS_RELWITHDEBINFO:" CMakeCache.txt | cut -d'=' -f2-)"
    echo "CXX Flags (RelWithDebInfo): $(grep "CMAKE_CXX_FLAGS_RELWITHDEBINFO:" CMakeCache.txt | cut -d'=' -f2-)"
  else
    echo "C Flags (Release): $(grep "CMAKE_C_FLAGS_RELEASE:" CMakeCache.txt | cut -d'=' -f2-)"
    echo "CXX Flags (Release): $(grep "CMAKE_CXX_FLAGS_RELEASE:" CMakeCache.txt | cut -d'=' -f2-)"
  fi
  
  echo ""
  echo "Linker Flags:"
  echo "EXE Linker: $(grep "CMAKE_EXE_LINKER_FLAGS:" CMakeCache.txt | cut -d'=' -f2-)"
  echo "Shared Linker: $(grep "CMAKE_SHARED_LINKER_FLAGS:" CMakeCache.txt | cut -d'=' -f2-)"
  echo "Module Linker: $(grep "CMAKE_MODULE_LINKER_FLAGS:" CMakeCache.txt | cut -d'=' -f2-)"
else
  echo "CMakeCache.txt not found - CMake configuration may have failed"
fi
echo "=================================="
echo ""

ninja
# sudo ninja install
