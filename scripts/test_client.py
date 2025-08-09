#!/usr/bin/env python3
"""
测试客户端
用于测试主节点API功能
"""
import asyncio
import aiohttp
import json
import time
from typing import List, Dict, Any


class VectorizationClient:
    """向量化客户端"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session: aiohttp.ClientSession = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        async with self.session.get(f"{self.base_url}/health") as response:
            return await response.json()
    
    async def submit_task(self, data: Any, task_type: str = "text_vectorization", **kwargs) -> Dict[str, Any]:
        """提交向量化任务"""
        payload = {
            "task_type": task_type,
            "data": data,
            **kwargs
        }
        
        async with self.session.post(
            f"{self.base_url}/tasks/submit",
            json=payload
        ) as response:
            return await response.json()
    
    async def get_task(self, task_id: str) -> Dict[str, Any]:
        """获取任务信息"""
        async with self.session.get(f"{self.base_url}/tasks/{task_id}") as response:
            return await response.json()
    
    async def list_tasks(self, **params) -> Dict[str, Any]:
        """获取任务列表"""
        async with self.session.get(f"{self.base_url}/tasks", params=params) as response:
            return await response.json()
    
    async def get_metrics(self) -> Dict[str, Any]:
        """获取系统指标"""
        async with self.session.get(f"{self.base_url}/metrics") as response:
            return await response.json()


async def test_basic_functionality():
    """测试基本功能"""
    print("=== 测试向量化系统基本功能 ===")
    
    async with VectorizationClient() as client:
        try:
            # 1. 健康检查
            print("\n1. 健康检查...")
            health = await client.health_check()
            print(f"健康状态: {health}")
            
            # 2. 提交文本向量化任务
            print("\n2. 提交文本向量化任务...")
            text_data = "这是一个测试文本，用于验证向量化功能。"
            task_result = await client.submit_task(
                data=text_data,
                task_type="text_vectorization",
                model_name="sentence-transformers/all-MiniLM-L6-v2",
                priority=1
            )
            print(f"任务提交结果: {task_result}")
            task_id = task_result.get("task_id")
            
            # 3. 查询任务状态
            print(f"\n3. 查询任务状态 (ID: {task_id})...")
            task_info = await client.get_task(task_id)
            print(f"任务信息: {task_info}")
            
            # 4. 提交批量任务
            print("\n4. 提交批量文本任务...")
            batch_data = [
                "第一个测试文本",
                "第二个测试文本", 
                "第三个测试文本"
            ]
            batch_result = await client.submit_task(
                data=batch_data,
                task_type="batch_vectorization",
                batch_size=2
            )
            print(f"批量任务结果: {batch_result}")
            
            # 5. 获取任务列表
            print("\n5. 获取任务列表...")
            task_list = await client.list_tasks(page=1, page_size=10)
            print(f"任务列表: {task_list}")
            
            # 6. 获取系统指标
            print("\n6. 获取系统指标...")
            metrics = await client.get_metrics()
            print(f"系统指标: {metrics}")
            
        except Exception as e:
            print(f"测试过程中出现错误: {e}")


async def test_batch_submission():
    """测试批量提交"""
    print("\n=== 测试批量任务提交 ===")
    
    async with VectorizationClient() as client:
        try:
            # 准备测试数据
            test_texts = [
                f"测试文本 {i}: 这是第{i}个测试句子，用于验证批量处理功能。"
                for i in range(1, 11)
            ]
            
            # 批量提交任务
            tasks = []
            for i, text in enumerate(test_texts):
                result = await client.submit_task(
                    data=text,
                    task_type="text_vectorization",
                    priority=i % 3 + 1,  # 随机优先级
                    metadata={"batch_id": "test_batch_1", "index": i}
                )
                tasks.append(result)
                print(f"提交任务 {i+1}: {result.get('task_id')}")
            
            # 等待一段时间
            print("\n等待3秒...")
            await asyncio.sleep(3)
            
            # 检查任务状态
            print("\n检查任务状态:")
            for i, task in enumerate(tasks):
                task_id = task.get("task_id")
                if task_id:
                    info = await client.get_task(task_id)
                    print(f"任务 {i+1} ({task_id}): {info.get('status')}")
            
        except Exception as e:
            print(f"批量测试过程中出现错误: {e}")


async def test_error_handling():
    """测试错误处理"""
    print("\n=== 测试错误处理 ===")
    
    async with VectorizationClient() as client:
        try:
            # 1. 提交空数据
            print("\n1. 测试空数据提交...")
            try:
                result = await client.submit_task(data="")
                print(f"空数据结果: {result}")
            except Exception as e:
                print(f"空数据错误 (预期): {e}")
            
            # 2. 查询不存在的任务
            print("\n2. 测试查询不存在的任务...")
            try:
                result = await client.get_task("non-existent-task-id")
                print(f"不存在任务结果: {result}")
            except Exception as e:
                print(f"不存在任务错误 (预期): {e}")
            
            # 3. 提交无效的任务类型
            print("\n3. 测试无效任务类型...")
            try:
                result = await client.submit_task(
                    data="test",
                    task_type="invalid_type"
                )
                print(f"无效类型结果: {result}")
            except Exception as e:
                print(f"无效类型错误 (预期): {e}")
                
        except Exception as e:
            print(f"错误处理测试过程中出现错误: {e}")


async def monitor_system():
    """监控系统状态"""
    print("\n=== 系统监控 ===")
    
    async with VectorizationClient() as client:
        try:
            for i in range(5):
                print(f"\n--- 监控轮次 {i+1} ---")
                
                # 健康检查
                health = await client.health_check()
                print(f"健康状态: {health.get('status')}")
                print(f"Kafka连接: {health.get('kafka_connected')}")
                print(f"Redis连接: {health.get('redis_connected')}")
                print(f"Weaviate连接: {health.get('weaviate_connected')}")
                print(f"活跃工作节点: {health.get('active_workers')}")
                print(f"队列大小: {health.get('queue_size')}")
                
                # 系统指标
                metrics = await client.get_metrics()
                print(f"总任务数: {metrics.get('total_tasks')}")
                print(f"待处理: {metrics.get('pending_tasks')}")
                print(f"处理中: {metrics.get('processing_tasks')}")
                print(f"已完成: {metrics.get('completed_tasks')}")
                print(f"失败: {metrics.get('failed_tasks')}")
                
                if i < 4:  # 最后一次不等待
                    await asyncio.sleep(10)
                    
        except Exception as e:
            print(f"监控过程中出现错误: {e}")


async def main():
    """主函数"""
    print("向量化系统测试客户端")
    print("=" * 50)
    
    try:
        # 基本功能测试
        await test_basic_functionality()
        
        # 批量提交测试
        await test_batch_submission()
        
        # 错误处理测试
        await test_error_handling()
        
        # 系统监控
        await monitor_system()
        
        print("\n" + "=" * 50)
        print("测试完成!")
        
    except Exception as e:
        print(f"测试失败: {e}")


if __name__ == "__main__":
    asyncio.run(main())