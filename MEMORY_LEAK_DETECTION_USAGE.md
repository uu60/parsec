# 内存泄漏检测使用指南

## 快速开始

### 1. 使用内存检测脚本（推荐）

```bash
# 运行完整的内存检测
./scripts/memory_check.sh

# 检测特定程序
./scripts/memory_check.sh simple_test

# 检测多个程序
./scripts/memory_check.sh simple_test test_multiply_comparison
```

### 2. 使用AddressSanitizer

```bash
# 创建ASan构建
mkdir build_asan && cd build_asan
cmake -DENABLE_ASAN=ON ..
make

# 运行程序
export ASAN_OPTIONS="detect_leaks=1:abort_on_error=1"
./your_program
```

### 3. 使用Valgrind

```bash
# 安装Valgrind (macOS)
brew install valgrind

# 创建Valgrind构建
mkdir build_valgrind && cd build_valgrind
cmake -DENABLE_VALGRIND_BUILD=ON ..
make

# 运行Valgrind检测
valgrind --tool=memcheck --leak-check=full ./your_program
```

### 4. 使用CMake内存检测配置

```bash
# 使用预配置的内存检测选项
mkdir build_memory && cd build_memory

# AddressSanitizer
cmake -DENABLE_ASAN=ON ..

# ThreadSanitizer (用于多线程程序)
cmake -DENABLE_TSAN=ON ..

# UndefinedBehaviorSanitizer
cmake -DENABLE_UBSAN=ON ..

make
```

## 针对您项目的特殊情况

### MPI程序检测

```bash
# 使用AddressSanitizer检测MPI程序
export ASAN_OPTIONS="detect_leaks=1"
mpirun -np 3 ./your_mpi_program

# 使用Valgrind检测MPI程序
mpirun -np 3 valgrind --tool=memcheck --leak-check=full ./your_mpi_program
```

### 当前发现的内存问题

从您的程序运行日志可以看到：
```
malloc_zone_error + 100
free_list_checksum_botch + 40
small_free_list_remove_ptr_no_clear + 964
free_small + 540
```

这表明存在：
1. **双重释放**：同一块内存被释放多次
2. **释放未分配的内存**：尝试释放不是通过malloc分配的内存
3. **内存损坏**：写入已释放的内存或越界写入

### 建议的检测顺序

1. **立即使用AddressSanitizer**：
   ```bash
   ./scripts/memory_check.sh
   ```

2. **如果ASan发现问题**，查看报告并修复

3. **使用Valgrind进行详细分析**：
   ```bash
   mkdir build_valgrind && cd build_valgrind
   cmake -DENABLE_VALGRIND_BUILD=ON ..
   make
   valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all ./benchmark_bg_vs_jit
   ```

4. **对于多线程问题，使用ThreadSanitizer**：
   ```bash
   mkdir build_tsan && cd build_tsan
   cmake -DENABLE_TSAN=ON ..
   make
   ./benchmark_bg_vs_jit
   ```

## 常见问题解决

### 如果程序崩溃
- 使用AddressSanitizer重新编译并运行
- 查看崩溃时的堆栈跟踪
- 检查是否有双重释放或使用已释放的内存

### 如果检测到内存泄漏
- 查看泄漏的堆栈跟踪
- 确认每个`new`都有对应的`delete`
- 检查异常安全性（使用RAII或智能指针）

### 性能影响
- AddressSanitizer：约2-3倍性能损失
- Valgrind：约10-50倍性能损失
- ThreadSanitizer：约5-15倍性能损失

## 修复建议

基于您的项目，建议：

1. **优先修复双重释放问题**
2. **检查BoolAndBatchOperator中的内存管理**
3. **考虑使用智能指针替代原始指针**
4. **添加单元测试来验证内存管理**

## 持续集成

将内存检测集成到CI/CD流程中：

```yaml
# .github/workflows/memory-check.yml
name: Memory Leak Detection
on: [push, pull_request]
jobs:
  memory-check:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Run Memory Check
      run: ./scripts/memory_check.sh
