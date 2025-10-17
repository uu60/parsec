#!/usr/bin/env python3
import os
import re
import sys


def remove_comments_safely(code: str) -> str:
    """
    Remove // and /*...*/ comments while keeping inline code intact,
    and preserve original blank lines.
    """

    # 处理多行注释 /* ... */
    def remove_block_comments(text: str) -> str:
        pattern = re.compile(r'/\*.*?\*/', re.DOTALL)
        return re.sub(pattern, '', text)

    # 先移除多行注释
    code = remove_block_comments(code)

    cleaned_lines = []
    for line in code.splitlines():
        stripped = line.strip()
        # 跳过纯注释行
        if stripped.startswith('//'):
            continue
        # 删除行尾注释，但保留前面的代码
        if '//' in line:
            # 找到 // 的位置，确保它不在字符串字面量里
            new_line = ''
            in_string = False
            for i, ch in enumerate(line):
                if ch == '"' and (i == 0 or line[i - 1] != '\\'):
                    in_string = not in_string
                if not in_string and line[i:i + 2] == '//':
                    new_line = line[:i]
                    break
            if not new_line:
                new_line = line
            line = new_line.rstrip()

        cleaned_lines.append(line)

    # 保留原有空行
    return '\n'.join(cleaned_lines) + '\n'


def clean_file(path: str):
    """Clean a single file in place."""
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            code = f.read()

        cleaned = remove_comments_safely(code)

        with open(path, 'w', encoding='utf-8') as f:
            f.write(cleaned)

        print(f"✅ Cleaned: {path}")
    except Exception as e:
        print(f"⚠️  Failed to clean {path}: {e}")


def scan_directory(root='.'):
    """Recursively clean all .cpp and .h files under root."""
    for dirpath, _, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith(('.cpp', '.h')):
                full_path = os.path.join(dirpath, filename)
                clean_file(full_path)


if __name__ == "__main__":
    root_dir = sys.argv[1] if len(sys.argv) > 1 else '.'
    print(f"🔍 Scanning directory: {os.path.abspath(root_dir)}")
    scan_directory(root_dir)
    print("🎉 Done! Comments removed, code and blank lines preserved.")
