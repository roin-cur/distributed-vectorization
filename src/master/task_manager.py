"""
任务管理器模块
负责任务的存储、状态管理和监控
"""
import json
import logging
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
import redis.asyncio as redis
import weaviate
from weaviate.exceptions import WeaviateException

from ..common.models import (
    VectorizationTask, 
    TaskStatus, 
    TaskType, 
    TaskMetrics,
    VectorizationResult
)
from ..common.kafka_producer import KafkaProducerManager
from .config import Settings

logger = logging.getLogger(__name__)


class TaskManager:
    """任务管理器"""
    
    def __init__(self, kafka_producer: KafkaProducerManager, settings: Settings):
        """
        初始化任务管理器
        
        Args:
            kafka_producer: Kafka生产者实例
            settings: 配置设置
        """
        self.kafka_producer = kafka_producer
        self.settings = settings
        
        # Redis连接
        self.redis_client: Optional[redis.Redis] = None
        
        # Weaviate连接
        self.weaviate_client: Optional[weaviate.Client] = None
        
        # 任务缓存
        self._task_cache: Dict[str, VectorizationTask] = {}
        self._cache_lock = asyncio.Lock()
        
        # 指标统计
        self._metrics = TaskMetrics()
        self._metrics_lock = asyncio.Lock()
    
    async def initialize(self) -> bool:
        """
        初始化任务管理器
        
        Returns:
            bool: 初始化是否成功
        """
        try:
            # 初始化Redis连接
            await self._init_redis()
            
            # 初始化Weaviate连接
            await self._init_weaviate()
            
            # 启动后台任务
            asyncio.create_task(self._metrics_updater())
            asyncio.create_task(self._task_cleaner())
            
            logger.info("Task manager initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize task manager: {e}")
            return False
    
    async def _init_redis(self):
        """初始化Redis连接"""
        try:
            self.redis_client = redis.from_url(
                self.settings.redis_url,
                max_connections=self.settings.redis_max_connections,
                socket_timeout=self.settings.redis_timeout,
                decode_responses=True
            )
            
            # 测试连接
            await self.redis_client.ping()
            logger.info("Redis connection established")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    async def _init_weaviate(self):
        """初始化Weaviate连接"""
        try:
            self.weaviate_client = weaviate.Client(
                url=self.settings.weaviate_url,
                timeout_config=weaviate.config.Timeout(
                    query=self.settings.weaviate_timeout,
                    insert=self.settings.weaviate_timeout
                )
            )
            
            # 测试连接
            if self.weaviate_client.is_ready():
                logger.info("Weaviate connection established")
            else:
                raise Exception("Weaviate is not ready")
                
        except Exception as e:
            logger.error(f"Failed to connect to Weaviate: {e}")
            raise
    
    async def save_task(self, task: VectorizationTask) -> bool:
        """
        保存任务到存储
        
        Args:
            task: 向量化任务
            
        Returns:
            bool: 保存是否成功
        """
        try:
            if not self.redis_client:
                logger.error("Redis client not initialized")
                return False
            
            # 序列化任务数据
            task_data = task.model_dump_json()
            
            # 保存到Redis
            key = f"task:{task.task_id}"
            await self.redis_client.setex(
                key, 
                timedelta(days=7).total_seconds(),  # 7天过期
                task_data
            )
            
            # 添加到任务索引
            await self._add_to_task_index(task)
            
            # 更新缓存
            async with self._cache_lock:
                self._task_cache[task.task_id] = task
            
            # 更新指标
            await self._update_metrics_for_new_task(task)
            
            logger.debug(f"Task {task.task_id} saved successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save task {task.task_id}: {e}")
            return False
    
    async def get_task(self, task_id: str) -> Optional[VectorizationTask]:
        """
        获取任务信息
        
        Args:
            task_id: 任务ID
            
        Returns:
            VectorizationTask: 任务对象，如果不存在返回None
        """
        try:
            # 先检查缓存
            async with self._cache_lock:
                if task_id in self._task_cache:
                    return self._task_cache[task_id]
            
            if not self.redis_client:
                return None
            
            # 从Redis获取
            key = f"task:{task_id}"
            task_data = await self.redis_client.get(key)
            
            if not task_data:
                return None
            
            # 反序列化任务数据
            task = VectorizationTask.model_validate_json(task_data)
            
            # 更新缓存
            async with self._cache_lock:
                self._task_cache[task_id] = task
            
            return task
            
        except Exception as e:
            logger.error(f"Failed to get task {task_id}: {e}")
            return None
    
    async def update_task_status(
        self, 
        task_id: str, 
        status: TaskStatus, 
        error_message: Optional[str] = None,
        progress: Optional[float] = None,
        worker_id: Optional[str] = None
    ) -> bool:
        """
        更新任务状态
        
        Args:
            task_id: 任务ID
            status: 新状态
            error_message: 错误信息
            progress: 进度
            worker_id: 工作节点ID
            
        Returns:
            bool: 更新是否成功
        """
        try:
            task = await self.get_task(task_id)
            if not task:
                logger.error(f"Task {task_id} not found")
                return False
            
            # 更新任务字段
            task.status = status
            if error_message:
                task.error_message = error_message
            if progress is not None:
                task.progress = progress
            if worker_id:
                task.worker_id = worker_id
            
            # 更新时间戳
            now = datetime.utcnow()
            if status == TaskStatus.PROCESSING and not task.started_at:
                task.started_at = now
            elif status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                task.completed_at = now
            
            # 保存更新后的任务
            await self.save_task(task)
            
            # 发送状态更新到Kafka
            await self.kafka_producer.send_task_status_update(
                task_id, 
                status.value,
                {
                    "progress": task.progress,
                    "worker_id": task.worker_id,
                    "error_message": task.error_message
                }
            )
            
            # 更新指标
            await self._update_metrics_for_status_change(task, status)
            
            logger.debug(f"Task {task_id} status updated to {status.value}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update task {task_id} status: {e}")
            return False
    
    async def list_tasks(
        self,
        status_filter: Optional[TaskStatus] = None,
        task_type_filter: Optional[TaskType] = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[VectorizationTask], int]:
        """
        获取任务列表
        
        Args:
            status_filter: 状态过滤
            task_type_filter: 类型过滤
            page: 页码
            page_size: 页大小
            
        Returns:
            Tuple[List[VectorizationTask], int]: 任务列表和总数
        """
        try:
            if not self.redis_client:
                return [], 0
            
            # 构建查询键
            pattern = "task:*"
            
            # 获取所有任务键
            task_keys = []
            async for key in self.redis_client.scan_iter(match=pattern):
                task_keys.append(key)
            
            # 获取任务数据
            tasks = []
            if task_keys:
                task_data_list = await self.redis_client.mget(task_keys)
                
                for task_data in task_data_list:
                    if task_data:
                        try:
                            task = VectorizationTask.model_validate_json(task_data)
                            
                            # 应用过滤器
                            if status_filter and task.status != status_filter:
                                continue
                            if task_type_filter and task.task_type != task_type_filter:
                                continue
                            
                            tasks.append(task)
                        except Exception as e:
                            logger.warning(f"Failed to parse task data: {e}")
                            continue
            
            # 排序（按创建时间倒序）
            tasks.sort(key=lambda x: x.created_at, reverse=True)
            
            # 分页
            total = len(tasks)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            paginated_tasks = tasks[start_idx:end_idx]
            
            return paginated_tasks, total
            
        except Exception as e:
            logger.error(f"Failed to list tasks: {e}")
            return [], 0
    
    async def estimate_processing_time(self, task: VectorizationTask) -> float:
        """
        估算任务处理时间
        
        Args:
            task: 向量化任务
            
        Returns:
            float: 估算的处理时间（秒）
        """
        try:
            # 基础处理时间（根据数据类型和大小）
            base_time = 1.0  # 秒
            
            # 根据数据大小调整
            if isinstance(task.data, str):
                data_factor = len(task.data) / 1000  # 每1000字符增加时间
            elif isinstance(task.data, list):
                data_factor = len(task.data) * 0.1  # 每个元素增加0.1秒
            else:
                data_factor = 1.0
            
            # 根据模型复杂度调整
            model_factor = 1.0
            if "large" in task.model_name.lower():
                model_factor = 2.0
            elif "base" in task.model_name.lower():
                model_factor = 1.5
            
            # 根据当前队列长度调整
            queue_size = await self.get_queue_size()
            queue_factor = 1.0 + (queue_size * 0.1)
            
            estimated_time = base_time * data_factor * model_factor * queue_factor
            
            return min(estimated_time, 3600)  # 最大不超过1小时
            
        except Exception as e:
            logger.error(f"Failed to estimate processing time: {e}")
            return 60.0  # 默认1分钟
    
    async def get_queue_size(self) -> int:
        """获取队列大小"""
        try:
            if not self.redis_client:
                return 0
            
            # 统计待处理和处理中的任务
            pending_count = 0
            processing_count = 0
            
            pattern = "task:*"
            async for key in self.redis_client.scan_iter(match=pattern):
                task_data = await self.redis_client.get(key)
                if task_data:
                    try:
                        task = VectorizationTask.model_validate_json(task_data)
                        if task.status == TaskStatus.PENDING:
                            pending_count += 1
                        elif task.status == TaskStatus.PROCESSING:
                            processing_count += 1
                    except Exception:
                        continue
            
            return pending_count + processing_count
            
        except Exception as e:
            logger.error(f"Failed to get queue size: {e}")
            return 0
    
    async def get_active_workers_count(self) -> int:
        """获取活跃工作节点数量"""
        try:
            if not self.redis_client:
                return 0
            
            # 从Redis获取活跃工作节点信息
            pattern = "worker:*:heartbeat"
            active_workers = 0
            
            async for key in self.redis_client.scan_iter(match=pattern):
                # 检查心跳时间，超过60秒认为不活跃
                heartbeat = await self.redis_client.get(key)
                if heartbeat:
                    try:
                        heartbeat_time = datetime.fromisoformat(heartbeat)
                        if (datetime.utcnow() - heartbeat_time).total_seconds() < 60:
                            active_workers += 1
                    except Exception:
                        continue
            
            return active_workers
            
        except Exception as e:
            logger.error(f"Failed to get active workers count: {e}")
            return 0
    
    async def get_metrics(self) -> TaskMetrics:
        """获取系统指标"""
        async with self._metrics_lock:
            return self._metrics.model_copy()
    
    async def check_weaviate_connection(self) -> bool:
        """检查Weaviate连接状态"""
        try:
            if not self.weaviate_client:
                return False
            return self.weaviate_client.is_ready()
        except Exception:
            return False
    
    async def check_redis_connection(self) -> bool:
        """检查Redis连接状态"""
        try:
            if not self.redis_client:
                return False
            await self.redis_client.ping()
            return True
        except Exception:
            return False
    
    async def _add_to_task_index(self, task: VectorizationTask):
        """添加任务到索引"""
        try:
            if not self.redis_client:
                return
            
            # 按状态索引
            status_key = f"tasks:status:{task.status.value}"
            await self.redis_client.sadd(status_key, task.task_id)
            await self.redis_client.expire(status_key, timedelta(days=7))
            
            # 按类型索引
            type_key = f"tasks:type:{task.task_type.value}"
            await self.redis_client.sadd(type_key, task.task_id)
            await self.redis_client.expire(type_key, timedelta(days=7))
            
        except Exception as e:
            logger.error(f"Failed to add task to index: {e}")
    
    async def _update_metrics_for_new_task(self, task: VectorizationTask):
        """为新任务更新指标"""
        async with self._metrics_lock:
            self._metrics.total_tasks += 1
            if task.status == TaskStatus.PENDING:
                self._metrics.pending_tasks += 1
    
    async def _update_metrics_for_status_change(self, task: VectorizationTask, new_status: TaskStatus):
        """为状态变更更新指标"""
        async with self._metrics_lock:
            # 减少旧状态计数
            if task.status == TaskStatus.PENDING:
                self._metrics.pending_tasks = max(0, self._metrics.pending_tasks - 1)
            elif task.status == TaskStatus.PROCESSING:
                self._metrics.processing_tasks = max(0, self._metrics.processing_tasks - 1)
            
            # 增加新状态计数
            if new_status == TaskStatus.PENDING:
                self._metrics.pending_tasks += 1
            elif new_status == TaskStatus.PROCESSING:
                self._metrics.processing_tasks += 1
            elif new_status == TaskStatus.COMPLETED:
                self._metrics.completed_tasks += 1
                # 更新处理时间
                if task.started_at and task.completed_at:
                    processing_time = (task.completed_at - task.started_at).total_seconds()
                    total_time = self._metrics.average_processing_time * (self._metrics.completed_tasks - 1)
                    self._metrics.average_processing_time = (total_time + processing_time) / self._metrics.completed_tasks
            elif new_status == TaskStatus.FAILED:
                self._metrics.failed_tasks += 1
    
    async def _metrics_updater(self):
        """后台指标更新任务"""
        while True:
            try:
                await asyncio.sleep(60)  # 每分钟更新一次
                
                # 重新计算指标
                await self._recalculate_metrics()
                
            except Exception as e:
                logger.error(f"Error in metrics updater: {e}")
    
    async def _task_cleaner(self):
        """后台任务清理器"""
        while True:
            try:
                await asyncio.sleep(3600)  # 每小时清理一次
                
                # 清理过期任务
                await self._clean_expired_tasks()
                
            except Exception as e:
                logger.error(f"Error in task cleaner: {e}")
    
    async def _recalculate_metrics(self):
        """重新计算指标"""
        try:
            if not self.redis_client:
                return
            
            # 重置指标
            metrics = TaskMetrics()
            total_processing_time = 0.0
            completed_count = 0
            
            # 遍历所有任务
            pattern = "task:*"
            async for key in self.redis_client.scan_iter(match=pattern):
                task_data = await self.redis_client.get(key)
                if task_data:
                    try:
                        task = VectorizationTask.model_validate_json(task_data)
                        
                        metrics.total_tasks += 1
                        
                        if task.status == TaskStatus.PENDING:
                            metrics.pending_tasks += 1
                        elif task.status == TaskStatus.PROCESSING:
                            metrics.processing_tasks += 1
                        elif task.status == TaskStatus.COMPLETED:
                            metrics.completed_tasks += 1
                            if task.started_at and task.completed_at:
                                processing_time = (task.completed_at - task.started_at).total_seconds()
                                total_processing_time += processing_time
                                completed_count += 1
                        elif task.status == TaskStatus.FAILED:
                            metrics.failed_tasks += 1
                            
                    except Exception:
                        continue
            
            # 计算平均处理时间
            if completed_count > 0:
                metrics.average_processing_time = total_processing_time / completed_count
            
            # 更新指标
            async with self._metrics_lock:
                self._metrics = metrics
                
        except Exception as e:
            logger.error(f"Failed to recalculate metrics: {e}")
    
    async def _clean_expired_tasks(self):
        """清理过期任务"""
        try:
            if not self.redis_client:
                return
            
            cutoff_time = datetime.utcnow() - timedelta(days=7)
            cleaned_count = 0
            
            pattern = "task:*"
            async for key in self.redis_client.scan_iter(match=pattern):
                task_data = await self.redis_client.get(key)
                if task_data:
                    try:
                        task = VectorizationTask.model_validate_json(task_data)
                        
                        # 清理7天前的已完成或失败任务
                        if (task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED] and
                            task.completed_at and task.completed_at < cutoff_time):
                            
                            await self.redis_client.delete(key)
                            cleaned_count += 1
                            
                            # 从缓存中移除
                            async with self._cache_lock:
                                self._task_cache.pop(task.task_id, None)
                                
                    except Exception:
                        continue
            
            if cleaned_count > 0:
                logger.info(f"Cleaned {cleaned_count} expired tasks")
                
        except Exception as e:
            logger.error(f"Failed to clean expired tasks: {e}")
    
    async def close(self):
        """关闭任务管理器"""
        try:
            if self.redis_client:
                await self.redis_client.close()
            
            logger.info("Task manager closed successfully")
            
        except Exception as e:
            logger.error(f"Error closing task manager: {e}")