# CMake配置文件：支持内存检测工具
# 使用方法: cmake -DCMAKE_TOOLCHAIN_FILE=CMakeMemoryCheck.cmake ..

# 内存检测选项
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_MSAN "Enable MemorySanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(ENABLE_VALGRIND_BUILD "Build for Valgrind (debug symbols, no optimization)" OFF)

# AddressSanitizer配置
if(ENABLE_ASAN)
    message(STATUS "启用AddressSanitizer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
    
    # 添加调试信息和优化级别
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O1 -fno-omit-frame-pointer")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O1 -fno-omit-frame-pointer")
    
    # 禁用优化以获得更好的堆栈跟踪
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-optimize-sibling-calls")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-optimize-sibling-calls")
endif()

# MemorySanitizer配置
if(ENABLE_MSAN)
    message(STATUS "启用MemorySanitizer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=memory")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=memory")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=memory")
    
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O1 -fno-omit-frame-pointer")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O1 -fno-omit-frame-pointer")
endif()

# ThreadSanitizer配置
if(ENABLE_TSAN)
    message(STATUS "启用ThreadSanitizer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
    
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O1")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O1")
endif()

# UndefinedBehaviorSanitizer配置
if(ENABLE_UBSAN)
    message(STATUS "启用UndefinedBehaviorSanitizer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=undefined")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
    
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O1")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O1")
endif()

# Valgrind构建配置
if(ENABLE_VALGRIND_BUILD)
    message(STATUS "为Valgrind构建（调试符号，无优化）")
    set(CMAKE_BUILD_TYPE "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O0")
    
    # 禁用内联以获得更好的堆栈跟踪
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-inline")
endif()

# 通用调试配置
if(ENABLE_ASAN OR ENABLE_MSAN OR ENABLE_TSAN OR ENABLE_UBSAN OR ENABLE_VALGRIND_BUILD)
    # 添加更多调试信息
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb3")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -ggdb3")
    
    # 保留符号表
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -rdynamic")
endif()

# 打印最终的编译标志
message(STATUS "最终的CXX编译标志: ${CMAKE_CXX_FLAGS}")
message(STATUS "最终的C编译标志: ${CMAKE_C_FLAGS}")
message(STATUS "最终的链接标志: ${CMAKE_EXE_LINKER_FLAGS}")
