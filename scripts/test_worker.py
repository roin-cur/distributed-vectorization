#!/usr/bin/env python3
"""
工作节点测试脚本
用于测试工作节点的各项功能
"""
import asyncio
import sys
import os
from typing import Dict, Any

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.worker.config import WorkerSettings, create_model_configs, create_vector_db_configs
from src.worker.model_manager import ModelManager
from src.worker.vector_db_manager import VectorDBManager
from src.common.models import VectorizationTask, TaskType
import numpy as np


async def test_model_manager():
    """测试模型管理器"""
    print("=== 测试模型管理器 ===")
    
    try:
        settings = WorkerSettings()
        model_configs = create_model_configs(settings)
        model_manager = ModelManager(model_configs, max_loaded_models=2)
        
        # 测试模型列表
        models_info = await model_manager.list_models()
        print(f"配置的模型数量: {len(models_info)}")
        for model_info in models_info[:3]:  # 只显示前3个
            print(f"  - {model_info['name']}: {model_info['type']}")
        
        # 测试加载模型
        print("\n测试加载模型...")
        model_name = settings.default_model
        print(f"加载模型: {model_name}")
        
        # 测试文本编码
        test_text = "这是一个测试文本，用于验证模型编码功能。"
        print(f"编码文本: {test_text}")
        
        vectors = await model_manager.encode_data(
            model_name, 
            test_text, 
            TaskType.TEXT_VECTORIZATION
        )
        
        print(f"编码结果: shape={vectors.shape}, dtype={vectors.dtype}")
        print(f"向量维度: {vectors.shape[-1]}")
        
        # 测试批量编码
        print("\n测试批量编码...")
        batch_texts = [
            "第一个测试文本",
            "第二个测试文本",
            "第三个测试文本"
        ]
        
        batch_vectors = await model_manager.encode_data(
            model_name,
            batch_texts,
            TaskType.BATCH_VECTORIZATION
        )
        
        print(f"批量编码结果: shape={batch_vectors.shape}")
        
        # 测试内存使用
        memory_info = model_manager.get_memory_usage()
        print(f"\n内存使用情况: {memory_info}")
        
        # 清理
        await model_manager.unload_all_models()
        print("所有模型已卸载")
        
        return True
        
    except Exception as e:
        print(f"模型管理器测试失败: {e}")
        return False


async def test_vector_db_manager():
    """测试向量数据库管理器"""
    print("\n=== 测试向量数据库管理器 ===")
    
    try:
        settings = WorkerSettings()
        db_configs = create_vector_db_configs(settings)
        db_manager = VectorDBManager(db_configs, settings.default_vector_db)
        
        # 列出数据库
        databases_info = await db_manager.list_databases()
        print(f"配置的数据库数量: {len(databases_info)}")
        for db_info in databases_info:
            print(f"  - {db_info['name']}: {db_info['type']}")
        
        # 测试连接（仅测试，不实际连接）
        print(f"\n尝试连接到默认数据库: {settings.default_vector_db}")
        
        try:
            db_info = await db_manager.get_database_info()
            print(f"数据库信息: {db_info}")
        except Exception as e:
            print(f"数据库连接失败 (预期): {e}")
        
        # 关闭连接
        await db_manager.close_all_connections()
        print("数据库连接已关闭")
        
        return True
        
    except Exception as e:
        print(f"向量数据库管理器测试失败: {e}")
        return False


async def test_integration():
    """集成测试"""
    print("\n=== 集成测试 ===")
    
    try:
        settings = WorkerSettings()
        
        # 初始化管理器
        model_configs = create_model_configs(settings)
        model_manager = ModelManager(model_configs, max_loaded_models=2)
        
        db_configs = create_vector_db_configs(settings)
        db_manager = VectorDBManager(db_configs, settings.default_vector_db)
        
        # 创建测试任务
        task = VectorizationTask(
            task_id="test-task-001",
            task_type=TaskType.TEXT_VECTORIZATION,
            data="这是一个集成测试文本，用于验证完整的处理流程。",
            model_name=settings.default_model,
            batch_size=1,
            priority=1
        )
        
        print(f"创建测试任务: {task.task_id}")
        print(f"任务类型: {task.task_type.value}")
        print(f"使用模型: {task.model_name}")
        
        # 编码文本
        print("\n执行向量化...")
        vectors = await model_manager.encode_data(
            task.model_name,
            task.data,
            task.task_type
        )
        
        print(f"向量化完成: shape={vectors.shape}")
        
        # 尝试存储（如果数据库可用）
        print("\n尝试存储向量...")
        try:
            success = await db_manager.store_vectors(task, vectors, task.data)
            if success:
                print("向量存储成功")
            else:
                print("向量存储失败")
        except Exception as e:
            print(f"向量存储失败 (预期): {e}")
        
        # 清理
        await model_manager.unload_all_models()
        await db_manager.close_all_connections()
        
        print("集成测试完成")
        return True
        
    except Exception as e:
        print(f"集成测试失败: {e}")
        return False


def test_configuration():
    """测试配置"""
    print("=== 测试配置 ===")
    
    try:
        settings = WorkerSettings()
        
        print(f"工作节点ID: {settings.worker_id}")
        print(f"Kafka服务器: {settings.kafka_bootstrap_servers}")
        print(f"默认模型: {settings.default_model}")
        print(f"设备: {settings.device}")
        print(f"最大并发任务: {settings.max_concurrent_tasks}")
        print(f"默认向量数据库: {settings.default_vector_db}")
        
        # 测试模型配置创建
        model_configs = create_model_configs(settings)
        print(f"模型配置数量: {len(model_configs)}")
        
        # 测试数据库配置创建
        db_configs = create_vector_db_configs(settings)
        print(f"数据库配置数量: {len(db_configs)}")
        
        print("配置测试通过")
        return True
        
    except Exception as e:
        print(f"配置测试失败: {e}")
        return False


async def main():
    """主函数"""
    print("工作节点功能测试")
    print("=" * 50)
    
    results = []
    
    # 配置测试
    results.append(test_configuration())
    
    # 模型管理器测试
    results.append(await test_model_manager())
    
    # 向量数据库管理器测试
    results.append(await test_vector_db_manager())
    
    # 集成测试
    results.append(await test_integration())
    
    # 总结
    print("\n" + "=" * 50)
    print("测试总结:")
    test_names = ["配置测试", "模型管理器测试", "向量数据库测试", "集成测试"]
    
    for i, (name, result) in enumerate(zip(test_names, results)):
        status = "✓ 通过" if result else "✗ 失败"
        print(f"  {name}: {status}")
    
    passed = sum(results)
    total = len(results)
    print(f"\n测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("所有测试通过! 🎉")
    else:
        print("部分测试失败，请检查配置和依赖。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n测试被用户中断")
    except Exception as e:
        print(f"测试运行失败: {e}")
        sys.exit(1)