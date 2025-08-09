#!/usr/bin/env python3
"""
主节点启动脚本
"""
import sys
import os

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.master import cli_main

if __name__ == "__main__":
    cli_main()