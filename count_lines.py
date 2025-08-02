#!/usr/bin/env python3
"""
统计从三个benchmark文件开始的所有依赖文件的代码行数
不重复计算相同的文件
"""

import os
import re
import sys
from pathlib import Path
from typing import Set, Dict, List

def extract_includes(file_path: str) -> List[str]:
    """从C++文件中提取#include语句"""
    includes = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            # 匹配 #include "..." 和 #include <...> 
            # 但我们主要关心项目内部的头文件，即用双引号的
            pattern = r'#include\s*["\<]([^"\>]+)["\>]'
            matches = re.findall(pattern, content)
            for match in matches:
                # 过滤掉标准库头文件
                if not is_system_header(match):
                    includes.append(match)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return includes

def is_system_header(header: str) -> bool:
    """判断是否为系统头文件"""
    system_headers = {
        'iostream', 'vector', 'string', 'map', 'set', 'algorithm', 
        'memory', 'fstream', 'sstream', 'iomanip', 'chrono', 'numeric',
        'future', 'thread', 'mutex', 'atomic', 'functional', 'utility',
        'cstdint', 'cstdlib', 'cstring', 'cmath', 'cfloat', 'climits',
        'cassert', 'exception', 'stdexcept', 'typeinfo', 'new', 'delete'
    }
    
    # 检查是否为标准库头文件
    if header in system_headers:
        return True
    
    # 检查是否以标准库前缀开头
    if (header.startswith('std') or header.startswith('boost') or 
        header.startswith('mpi') or header.startswith('tbb') or
        header.endswith('.hpp') and 'third_party' in header):
        return True
    
    return False

def find_header_file(header: str, base_dir: str) -> str:
    """在项目中查找头文件的实际路径"""
    # 可能的搜索路径
    search_paths = [
        os.path.join(base_dir, 'primitives', 'include'),
        os.path.join(base_dir, 'db', 'include'),
        os.path.join(base_dir, 'primitives', 'src'),
        os.path.join(base_dir, 'db', 'src'),
        base_dir
    ]
    
    for search_path in search_paths:
        potential_path = os.path.join(search_path, header)
        if os.path.exists(potential_path):
            return potential_path
    
    return None

def find_source_file(header_path: str, base_dir: str) -> str:
    """根据头文件路径查找对应的源文件"""
    if not header_path:
        return None
    
    # 将.h/.hpp转换为.cpp/.c
    base_name = os.path.splitext(header_path)[0]
    
    # 可能的源文件扩展名
    source_extensions = ['.cpp', '.c', '.cc', '.cxx']
    
    for ext in source_extensions:
        source_path = base_name + ext
        if os.path.exists(source_path):
            return source_path
    
    # 如果头文件在include目录下，尝试在src目录下查找
    if 'include' in header_path:
        src_path = header_path.replace('include', 'src')
        for ext in source_extensions:
            source_path = os.path.splitext(src_path)[0] + ext
            if os.path.exists(source_path):
                return source_path
    
    return None

def count_lines(file_path: str) -> int:
    """统计文件行数"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return len(f.readlines())
    except Exception as e:
        print(f"Error counting lines in {file_path}: {e}")
        return 0

def analyze_dependencies(start_files: List[str], base_dir: str) -> Dict[str, int]:
    """递归分析依赖关系并统计行数"""
    processed_files = set()
    file_lines = {}
    to_process = list(start_files)
    
    while to_process:
        current_file = to_process.pop(0)
        
        # 如果已经处理过，跳过
        if current_file in processed_files:
            continue
        
        processed_files.add(current_file)
        
        # 统计当前文件行数
        if os.path.exists(current_file):
            lines = count_lines(current_file)
            file_lines[current_file] = lines
            print(f"Processing: {current_file} ({lines} lines)")
            
            # 提取包含的头文件
            includes = extract_includes(current_file)
            
            for include in includes:
                # 查找头文件
                header_path = find_header_file(include, base_dir)
                if header_path and header_path not in processed_files:
                    to_process.append(header_path)
                
                # 查找对应的源文件
                source_path = find_source_file(header_path, base_dir)
                if source_path and source_path not in processed_files:
                    to_process.append(source_path)
        else:
            print(f"File not found: {current_file}")
    
    return file_lines

def main():
    # 获取当前工作目录
    base_dir = os.getcwd()
    
    # 三个起始benchmark文件
    start_files = [
        os.path.join(base_dir, 'primitives/benchmark/benchmark_bg_vs_jit.cpp'),
        os.path.join(base_dir, 'primitives/benchmark/benchmark_arith_vs_bool.cpp'),
        os.path.join(base_dir, 'primitives/benchmark/benchmark_dyn_batch_size.cpp')
    ]
    
    print("开始分析依赖关系...")
    print("起始文件:")
    for f in start_files:
        print(f"  {f}")
    print()
    
    # 分析依赖关系并统计行数
    file_lines = analyze_dependencies(start_files, base_dir)
    
    # 输出结果
    print("\n" + "="*80)
    print("统计结果:")
    print("="*80)
    
    total_lines = 0
    file_count = 0
    
    # 按文件类型分类
    cpp_files = []
    h_files = []
    
    for file_path, lines in sorted(file_lines.items()):
        total_lines += lines
        file_count += 1
        
        if file_path.endswith(('.cpp', '.c', '.cc', '.cxx')):
            cpp_files.append((file_path, lines))
        elif file_path.endswith(('.h', '.hpp')):
            h_files.append((file_path, lines))
    
    print(f"总文件数: {file_count}")
    print(f"总代码行数: {total_lines}")
    print()
    
    print(f"源文件 (.cpp/.c) 数量: {len(cpp_files)}")
    cpp_lines = sum(lines for _, lines in cpp_files)
    print(f"源文件总行数: {cpp_lines}")
    print()
    
    print(f"头文件 (.h/.hpp) 数量: {len(h_files)}")
    h_lines = sum(lines for _, lines in h_files)
    print(f"头文件总行数: {h_lines}")
    print()
    
    print("详细文件列表:")
    print("-" * 80)
    print("源文件:")
    for file_path, lines in cpp_files:
        rel_path = os.path.relpath(file_path, base_dir)
        print(f"  {rel_path:<60} {lines:>6} 行")
    
    print("\n头文件:")
    for file_path, lines in h_files:
        rel_path = os.path.relpath(file_path, base_dir)
        print(f"  {rel_path:<60} {lines:>6} 行")
    
    # 保存结果到文件
    output_file = "code_lines_analysis.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("代码行数统计分析\n")
        f.write("="*80 + "\n")
        f.write(f"分析时间: {os.popen('date').read().strip()}\n")
        f.write(f"基础目录: {base_dir}\n\n")
        
        f.write("起始文件:\n")
        for start_file in start_files:
            f.write(f"  {os.path.relpath(start_file, base_dir)}\n")
        f.write("\n")
        
        f.write("统计结果:\n")
        f.write(f"总文件数: {file_count}\n")
        f.write(f"总代码行数: {total_lines}\n")
        f.write(f"源文件数量: {len(cpp_files)} (共 {cpp_lines} 行)\n")
        f.write(f"头文件数量: {len(h_files)} (共 {h_lines} 行)\n\n")
        
        f.write("详细文件列表:\n")
        f.write("-" * 80 + "\n")
        f.write("源文件:\n")
        for file_path, lines in cpp_files:
            rel_path = os.path.relpath(file_path, base_dir)
            f.write(f"  {rel_path:<60} {lines:>6} 行\n")
        
        f.write("\n头文件:\n")
        for file_path, lines in h_files:
            rel_path = os.path.relpath(file_path, base_dir)
            f.write(f"  {rel_path:<60} {lines:>6} 行\n")
    
    print(f"\n详细结果已保存到: {output_file}")

if __name__ == "__main__":
    main()
