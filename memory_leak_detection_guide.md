# C++项目内存泄漏检测工具指南

## 1. Valgrind (推荐 - Linux/macOS)

### 安装
```bash
# macOS
brew install valgrind

# Ubuntu/Debian
sudo apt-get install valgrind

# CentOS/RHEL
sudo yum install valgrind
```

### 使用方法
```bash
# 基本内存泄漏检测
valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all ./your_program

# 详细报告
valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --verbose ./your_program

# 生成报告文件
valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --log-file=valgrind_report.txt ./your_program
```

### 编译选项
```bash
# 编译时添加调试信息
g++ -g -O0 -DDEBUG your_files.cpp -o your_program
```

## 2. AddressSanitizer (ASan) - 推荐

### 编译选项
```bash
# GCC/Clang
g++ -fsanitize=address -g -O1 your_files.cpp -o your_program
clang++ -fsanitize=address -g -O1 your_files.cpp -o your_program
```

### 运行时环境变量
```bash
export ASAN_OPTIONS=detect_leaks=1:abort_on_error=1:print_stats=1
./your_program
```

## 3. Dr. Memory (Windows/Linux)

### 安装
```bash
# 下载并安装 Dr. Memory
# https://drmemory.org/

# 使用
drmemory -- ./your_program
```

## 4. Clang Static Analyzer

### 使用
```bash
# 静态分析
clang --analyze your_files.cpp

# 或使用scan-build
scan-build make
```

## 5. 项目集成方案

### CMake集成AddressSanitizer
```cmake
# 添加到CMakeLists.txt
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)

if(ENABLE_ASAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -g -O1")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -g -O1")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()
```

### 使用方法
```bash
mkdir build && cd build
cmake -DENABLE_ASAN=ON ..
make
./your_program
```

## 6. 自动化检测脚本

### Valgrind检测脚本
```bash
#!/bin/bash
# valgrind_check.sh

PROGRAM=$1
if [ -z "$PROGRAM" ]; then
    echo "Usage: $0 <program_path>"
    exit 1
fi

echo "Running Valgrind memory leak detection..."
valgrind --tool=memcheck \
         --leak-check=full \
         --show-leak-kinds=all \
         --track-origins=yes \
         --verbose \
         --log-file=valgrind_$(date +%Y%m%d_%H%M%S).log \
         $PROGRAM

echo "Valgrind report saved to valgrind_*.log"
```

## 7. 针对您的项目的具体建议

### 对于MPI项目的特殊考虑
```bash
# MPI程序的Valgrind检测
mpirun -np 3 valgrind --tool=memcheck --leak-check=full ./your_mpi_program

# 或者单独检测每个进程
valgrind --tool=memcheck --leak-check=full mpirun -np 1 ./your_mpi_program
```

### 编译您的项目进行内存检测
```bash
# 修改编译选项
export CXXFLAGS="-g -O0 -fsanitize=address"
export LDFLAGS="-fsanitize=address"

# 重新编译
make clean
make

# 运行测试
export ASAN_OPTIONS=detect_leaks=1
./your_test_program
```

## 8. 持续集成中的内存检测

### GitHub Actions示例
```yaml
name: Memory Leak Detection
on: [push, pull_request]

jobs:
  memory-check:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Install Valgrind
      run: sudo apt-get install valgrind
    - name: Build with debug info
      run: |
        mkdir build && cd build
        cmake -DCMAKE_BUILD_TYPE=Debug ..
        make
    - name: Run Valgrind
      run: |
        cd build
        valgrind --tool=memcheck --leak-check=full --error-exitcode=1 ./your_program
```

## 9. 推荐的检测流程

1. **开发阶段**: 使用AddressSanitizer进行快速检测
2. **测试阶段**: 使用Valgrind进行详细分析
3. **CI/CD**: 集成自动化内存检测
4. **发布前**: 使用多种工具交叉验证

## 10. 常见内存问题模式检测

### 检测new/delete不匹配
```cpp
// 在代码中添加检测宏
#ifdef DEBUG
#define NEW_DEBUG new(__FILE__, __LINE__)
#define DELETE_DEBUG delete
#else
#define NEW_DEBUG new
#define DELETE_DEBUG delete
#endif
```

### 智能指针迁移检测
```bash
# 搜索可能的内存泄漏模式
grep -r "new.*\*" --include="*.cpp" --include="*.h" .
grep -r "delete" --include="*.cpp" --include="*.h" .
