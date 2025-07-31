#!/bin/bash

# 内存泄漏检测脚本 for parsec项目
# 使用方法: ./scripts/memory_check.sh [program_name]

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 默认程序列表
DEFAULT_PROGRAMS=(
    "simple_test"
    "test_multiply_comparison"
    "db/client/main"
    "primitives/benchmark/algorithm_validate"
    "primitives/benchmark/benchmark_arith_vs_bool"
)

# 检查是否安装了必要工具
check_tools() {
    echo -e "${YELLOW}检查内存检测工具...${NC}"
    
    if command -v valgrind &> /dev/null; then
        echo -e "${GREEN}✓ Valgrind 已安装${NC}"
        VALGRIND_AVAILABLE=true
    else
        echo -e "${RED}✗ Valgrind 未安装${NC}"
        VALGRIND_AVAILABLE=false
    fi
    
    # 检查是否支持AddressSanitizer
    if gcc -fsanitize=address -x c /dev/null -o /dev/null 2>/dev/null; then
        echo -e "${GREEN}✓ AddressSanitizer 支持${NC}"
        ASAN_AVAILABLE=true
    else
        echo -e "${RED}✗ AddressSanitizer 不支持${NC}"
        ASAN_AVAILABLE=false
    fi
    
    rm -f /dev/null.o 2>/dev/null || true
}

# 使用AddressSanitizer编译
build_with_asan() {
    echo -e "${YELLOW}使用AddressSanitizer编译项目...${NC}"
    
    # 创建构建目录
    mkdir -p build_asan
    cd build_asan
    
    # 配置CMake
    cmake -DCMAKE_BUILD_TYPE=Debug \
          -DCMAKE_CXX_FLAGS="-fsanitize=address -g -O1 -fno-omit-frame-pointer" \
          -DCMAKE_C_FLAGS="-fsanitize=address -g -O1 -fno-omit-frame-pointer" \
          -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address" \
          ..
    
    # 编译
    make -j$(nproc)
    cd ..
    
    echo -e "${GREEN}✓ AddressSanitizer构建完成${NC}"
}

# 使用Valgrind编译
build_for_valgrind() {
    echo -e "${YELLOW}为Valgrind编译项目...${NC}"
    
    # 创建构建目录
    mkdir -p build_valgrind
    cd build_valgrind
    
    # 配置CMake
    cmake -DCMAKE_BUILD_TYPE=Debug \
          -DCMAKE_CXX_FLAGS="-g -O0" \
          -DCMAKE_C_FLAGS="-g -O0" \
          ..
    
    # 编译
    make -j$(nproc)
    cd ..
    
    echo -e "${GREEN}✓ Valgrind构建完成${NC}"
}

# 运行AddressSanitizer检测
run_asan_check() {
    local program=$1
    echo -e "${YELLOW}运行AddressSanitizer检测: $program${NC}"
    
    export ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:print_stats=1:log_path=./asan_report"
    export MSAN_OPTIONS="print_stats=1"
    
    cd build_asan
    
    if [ -f "$program" ]; then
        echo "运行程序: $program"
        timeout 60s ./$program || {
            echo -e "${RED}程序执行超时或出错${NC}"
            return 1
        }
    else
        echo -e "${RED}程序不存在: $program${NC}"
        return 1
    fi
    
    cd ..
    
    # 检查是否有ASan报告
    if ls asan_report.* 1> /dev/null 2>&1; then
        echo -e "${RED}发现内存错误，查看 asan_report.* 文件${NC}"
        return 1
    else
        echo -e "${GREEN}✓ AddressSanitizer检测通过${NC}"
        return 0
    fi
}

# 运行Valgrind检测
run_valgrind_check() {
    local program=$1
    echo -e "${YELLOW}运行Valgrind检测: $program${NC}"
    
    cd build_valgrind
    
    if [ -f "$program" ]; then
        local report_file="../valgrind_report_$(basename $program)_$(date +%Y%m%d_%H%M%S).log"
        
        echo "运行Valgrind检测..."
        timeout 120s valgrind \
            --tool=memcheck \
            --leak-check=full \
            --show-leak-kinds=all \
            --track-origins=yes \
            --verbose \
            --log-file="$report_file" \
            ./$program
        
        # 分析结果
        if grep -q "ERROR SUMMARY: 0 errors" "$report_file"; then
            echo -e "${GREEN}✓ Valgrind检测通过${NC}"
            rm "$report_file"
            cd ..
            return 0
        else
            echo -e "${RED}发现内存错误，查看报告: $report_file${NC}"
            echo "错误摘要:"
            grep "ERROR SUMMARY\|LEAK SUMMARY" "$report_file" || true
            cd ..
            return 1
        fi
    else
        echo -e "${RED}程序不存在: $program${NC}"
        cd ..
        return 1
    fi
}

# 静态分析检测
run_static_analysis() {
    echo -e "${YELLOW}运行静态分析...${NC}"
    
    # 查找可能的内存泄漏模式
    echo "搜索可能的内存泄漏模式..."
    
    echo "=== new/delete 配对检查 ==="
    NEW_COUNT=$(grep -r "new " --include="*.cpp" --include="*.h" . | wc -l)
    DELETE_COUNT=$(grep -r "delete " --include="*.cpp" --include="*.h" . | wc -l)
    
    echo "new 语句数量: $NEW_COUNT"
    echo "delete 语句数量: $DELETE_COUNT"
    
    if [ $NEW_COUNT -gt $DELETE_COUNT ]; then
        echo -e "${YELLOW}警告: new语句多于delete语句${NC}"
    fi
    
    echo "=== 可能的内存泄漏点 ==="
    grep -rn "new.*\*" --include="*.cpp" --include="*.h" . | head -10
    
    echo "=== 智能指针使用情况 ==="
    SMART_PTR_COUNT=$(grep -r "std::.*_ptr\|std::shared_ptr\|std::unique_ptr" --include="*.cpp" --include="*.h" . | wc -l)
    echo "智能指针使用次数: $SMART_PTR_COUNT"
}

# 主函数
main() {
    echo -e "${GREEN}=== Parsec项目内存泄漏检测 ===${NC}"
    
    # 检查工具
    check_tools
    
    # 确定要检测的程序
    if [ $# -eq 0 ]; then
        PROGRAMS=("${DEFAULT_PROGRAMS[@]}")
        echo "使用默认程序列表进行检测"
    else
        PROGRAMS=("$@")
        echo "检测指定程序: $@"
    fi
    
    # 运行静态分析
    run_static_analysis
    
    # 如果支持AddressSanitizer，优先使用
    if [ "$ASAN_AVAILABLE" = true ]; then
        build_with_asan
        
        echo -e "${YELLOW}=== AddressSanitizer 检测 ===${NC}"
        ASAN_FAILED=0
        for program in "${PROGRAMS[@]}"; do
            if ! run_asan_check "$program"; then
                ASAN_FAILED=$((ASAN_FAILED + 1))
            fi
        done
        
        if [ $ASAN_FAILED -eq 0 ]; then
            echo -e "${GREEN}✓ 所有程序通过AddressSanitizer检测${NC}"
        else
            echo -e "${RED}✗ $ASAN_FAILED 个程序未通过AddressSanitizer检测${NC}"
        fi
    fi
    
    # 如果支持Valgrind，也运行Valgrind检测
    if [ "$VALGRIND_AVAILABLE" = true ]; then
        build_for_valgrind
        
        echo -e "${YELLOW}=== Valgrind 检测 ===${NC}"
        VALGRIND_FAILED=0
        for program in "${PROGRAMS[@]}"; do
            if ! run_valgrind_check "$program"; then
                VALGRIND_FAILED=$((VALGRIND_FAILED + 1))
            fi
        done
        
        if [ $VALGRIND_FAILED -eq 0 ]; then
            echo -e "${GREEN}✓ 所有程序通过Valgrind检测${NC}"
        else
            echo -e "${RED}✗ $VALGRIND_FAILED 个程序未通过Valgrind检测${NC}"
        fi
    fi
    
    # 总结
    echo -e "${GREEN}=== 检测完成 ===${NC}"
    if [ "$ASAN_AVAILABLE" = false ] && [ "$VALGRIND_AVAILABLE" = false ]; then
        echo -e "${RED}警告: 没有可用的内存检测工具${NC}"
        echo "请安装Valgrind或确保编译器支持AddressSanitizer"
    fi
}

# 运行主函数
main "$@"
