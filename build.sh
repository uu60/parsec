#!/bin/sh

SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd -P) || exit 1
BUILD_DIR="$SCRIPT_DIR/build"
# Parse command line arguments
USE_ASAN=false
OPT_LEVEL=O3
while [ "$#" -gt 0 ]; do
  case "$1" in
    --asan)
      USE_ASAN=true
      shift
      ;;
    --O2)
      OPT_LEVEL=O2
      shift
      ;;
    --O3)
      OPT_LEVEL=O3
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [--asan] [--O2|--O3] [--help]"
      echo "  --asan    Enable AddressSanitizer build"
      echo "  --O2      Use -O2 for Release build"
      echo "  --O3      Use -O3 for Release build (default)"
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

cd "$SCRIPT_DIR" || exit 1
mkdir -p "$BUILD_DIR" || exit 1

# Configure CMake with or without ASAN
if [ "$USE_ASAN" = true ]; then
  echo "Building with AddressSanitizer enabled (O1)…"
  cmake -S "$SCRIPT_DIR" -B "$BUILD_DIR" -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_C_FLAGS_RELWITHDEBINFO="-O1 -g -fno-omit-frame-pointer -fsanitize=address" \
    -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O1 -g -fno-omit-frame-pointer -fsanitize=address" \
    -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address" \
    -DCMAKE_SHARED_LINKER_FLAGS="-fsanitize=address" \
    -DCMAKE_MODULE_LINKER_FLAGS="-fsanitize=address"
else
  echo "Building without AddressSanitizer (Using ${OPT_LEVEL})…"
  cmake -S "$SCRIPT_DIR" -B "$BUILD_DIR" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_FLAGS_RELEASE="-${OPT_LEVEL} -DNDEBUG" \
    -DCMAKE_CXX_FLAGS_RELEASE="-${OPT_LEVEL} -DNDEBUG"
fi

# Print actual CMake configuration after cmake runs
echo ""
echo "=== Actual CMake Configuration ==="
if [ -f "$BUILD_DIR/CMakeCache.txt" ]; then
  echo "Build Type: $(grep "CMAKE_BUILD_TYPE:" "$BUILD_DIR/CMakeCache.txt" | cut -d'=' -f2)"
  echo "C Compiler: $(grep "CMAKE_C_COMPILER:" "$BUILD_DIR/CMakeCache.txt" | cut -d'=' -f2)"
  echo "CXX Compiler: $(grep "CMAKE_CXX_COMPILER:" "$BUILD_DIR/CMakeCache.txt" | cut -d'=' -f2)"
  
  # Extract actual flags being used
  echo ""
  echo "Actual Compiler Flags:"
  if [ "$USE_ASAN" = true ]; then
    echo "C Flags (RelWithDebInfo): $(grep "CMAKE_C_FLAGS_RELWITHDEBINFO:" "$BUILD_DIR/CMakeCache.txt" | cut -d'=' -f2-)"
    echo "CXX Flags (RelWithDebInfo): $(grep "CMAKE_CXX_FLAGS_RELWITHDEBINFO:" "$BUILD_DIR/CMakeCache.txt" | cut -d'=' -f2-)"
  else
    echo "C Flags (Release): $(grep "CMAKE_C_FLAGS_RELEASE:" "$BUILD_DIR/CMakeCache.txt" | cut -d'=' -f2-)"
    echo "CXX Flags (Release): $(grep "CMAKE_CXX_FLAGS_RELEASE:" "$BUILD_DIR/CMakeCache.txt" | cut -d'=' -f2-)"
  fi
  
  echo ""
  echo "Linker Flags:"
  echo "EXE Linker: $(grep "CMAKE_EXE_LINKER_FLAGS:" "$BUILD_DIR/CMakeCache.txt" | cut -d'=' -f2-)"
  echo "Shared Linker: $(grep "CMAKE_SHARED_LINKER_FLAGS:" "$BUILD_DIR/CMakeCache.txt" | cut -d'=' -f2-)"
  echo "Module Linker: $(grep "CMAKE_MODULE_LINKER_FLAGS:" "$BUILD_DIR/CMakeCache.txt" | cut -d'=' -f2-)"
else
  echo "CMakeCache.txt not found - CMake configuration may have failed"
fi
echo "=================================="
echo ""

cmake --build "$BUILD_DIR"
# sudo ninja install
