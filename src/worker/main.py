"""
工作节点主应用程序
负责启动和管理向量化工作节点
"""
import asyncio
import logging
import signal
import sys
import os
import time
from typing import Optional
from datetime import datetime
import redis.asyncio as redis
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from .config import WorkerSettings, create_model_configs, create_vector_db_configs, validate_settings, print_settings
from .model_manager import ModelManager
from .vector_db_manager import VectorDBManager
from .task_processor import TaskProcessor
from ..common.kafka_consumer import KafkaConsumerManager, TaskConsumer, ResultProducer

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus指标
TASKS_PROCESSED = Counter('worker_tasks_processed_total', 'Total processed tasks', ['worker_id', 'status'])
TASK_PROCESSING_TIME = Histogram('worker_task_processing_seconds', 'Task processing time', ['worker_id', 'task_type'])
LOADED_MODELS = Gauge('worker_loaded_models', 'Number of loaded models', ['worker_id'])
MEMORY_USAGE = Gauge('worker_memory_usage_bytes', 'Memory usage in bytes', ['worker_id'])
ACTIVE_TASKS = Gauge('worker_active_tasks', 'Number of active tasks', ['worker_id'])


class WorkerNodeService:
    """工作节点服务类"""
    
    def __init__(self):
        self.settings = WorkerSettings()
        self.kafka_consumer: Optional[KafkaConsumerManager] = None
        self.task_consumer: Optional[TaskConsumer] = None
        self.result_producer: Optional[ResultProducer] = None
        self.model_manager: Optional[ModelManager] = None
        self.vector_db_manager: Optional[VectorDBManager] = None
        self.task_processor: Optional[TaskProcessor] = None
        self.redis_client: Optional[redis.Redis] = None
        
        # 运行状态
        self.is_running = False
        self.shutdown_event = asyncio.Event()
        self.active_tasks = 0
        self.max_active_tasks = self.settings.max_concurrent_tasks
        
        # 信号处理
        self._setup_signal_handlers()
        
        # 心跳任务
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.metrics_task: Optional[asyncio.Task] = None
    
    def _setup_signal_handlers(self):
        """设置信号处理器"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            asyncio.create_task(self.shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def initialize(self) -> bool:
        """
        初始化工作节点服务
        
        Returns:
            bool: 初始化是否成功
        """
        try:
            logger.info(f"Initializing Worker Node Service: {self.settings.worker_id}")
            
            # 验证配置
            if not validate_settings(self.settings):
                logger.error("Configuration validation failed")
                return False
            
            # 打印配置信息
            print_settings(self.settings)
            
            # 配置日志级别
            logging.getLogger().setLevel(getattr(logging, self.settings.log_level.upper()))
            
            # 创建模型缓存目录
            os.makedirs(self.settings.models_cache_dir, exist_ok=True)
            
            # 初始化Redis连接（用于心跳）
            await self._init_redis()
            
            # 初始化模型管理器
            logger.info("Initializing model manager...")
            model_configs = create_model_configs(self.settings)
            self.model_manager = ModelManager(model_configs, self.settings.max_loaded_models)
            
            # 初始化向量数据库管理器
            logger.info("Initializing vector database manager...")
            db_configs = create_vector_db_configs(self.settings)
            self.vector_db_manager = VectorDBManager(db_configs, self.settings.default_vector_db)
            
            # 初始化Kafka消费者
            logger.info("Initializing Kafka consumer...")
            self.kafka_consumer = KafkaConsumerManager(
                bootstrap_servers=self.settings.kafka_bootstrap_servers,
                group_id=self.settings.kafka_group_id,
                worker_id=self.settings.worker_id
            )
            
            if not await self.kafka_consumer.initialize():
                logger.error("Failed to initialize Kafka consumer")
                return False
            
            # 初始化结果生产者
            logger.info("Initializing result producer...")
            self.result_producer = ResultProducer(self.kafka_consumer)
            if not await self.result_producer.initialize():
                logger.error("Failed to initialize result producer")
                return False
            
            # 初始化任务处理器
            logger.info("Initializing task processor...")
            self.task_processor = TaskProcessor(
                self.model_manager,
                self.vector_db_manager,
                self.result_producer,
                self.settings.worker_id
            )
            
            # 初始化任务消费者
            self.task_consumer = TaskConsumer(self.kafka_consumer, self._process_task_wrapper)
            
            # 启动Prometheus指标服务器
            if self.settings.enable_metrics:
                await self._start_metrics_server()
            
            # 启动后台任务
            self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self.metrics_task = asyncio.create_task(self._metrics_loop())
            
            logger.info("Worker Node Service initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Worker Node Service: {e}")
            return False
    
    async def _init_redis(self):
        """初始化Redis连接"""
        try:
            self.redis_client = redis.from_url(
                self.settings.redis_url,
                socket_timeout=self.settings.redis_timeout,
                decode_responses=True
            )
            
            # 测试连接
            await self.redis_client.ping()
            logger.info("Redis connection established")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    async def _start_metrics_server(self):
        """启动Prometheus指标服务器"""
        try:
            start_http_server(self.settings.metrics_port)
            logger.info(f"Prometheus metrics server started on port {self.settings.metrics_port}")
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")
    
    async def _process_task_wrapper(self, task):
        """任务处理包装器，用于并发控制和指标收集"""
        # 检查并发限制
        if self.active_tasks >= self.max_active_tasks:
            logger.warning(f"Max concurrent tasks ({self.max_active_tasks}) reached, task {task.task_id} will wait")
            
            # 等待有空闲槽位
            while self.active_tasks >= self.max_active_tasks:
                await asyncio.sleep(1)
        
        self.active_tasks += 1
        ACTIVE_TASKS.labels(worker_id=self.settings.worker_id).set(self.active_tasks)
        
        start_time = time.time()
        
        try:
            # 处理任务
            result = await self.task_processor.process_task(task)
            
            # 更新指标
            processing_time = time.time() - start_time
            TASKS_PROCESSED.labels(
                worker_id=self.settings.worker_id,
                status='success'
            ).inc()
            TASK_PROCESSING_TIME.labels(
                worker_id=self.settings.worker_id,
                task_type=task.task_type.value
            ).observe(processing_time)
            
            return result
            
        except Exception as e:
            # 更新失败指标
            TASKS_PROCESSED.labels(
                worker_id=self.settings.worker_id,
                status='failed'
            ).inc()
            
            logger.error(f"Task processing failed: {e}")
            raise
            
        finally:
            self.active_tasks -= 1
            ACTIVE_TASKS.labels(worker_id=self.settings.worker_id).set(self.active_tasks)
    
    async def _heartbeat_loop(self):
        """心跳循环"""
        while not self.shutdown_event.is_set():
            try:
                if self.redis_client:
                    # 发送心跳
                    heartbeat_key = f"worker:{self.settings.worker_id}:heartbeat"
                    heartbeat_data = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "status": "active",
                        "active_tasks": self.active_tasks,
                        "loaded_models": len(self.model_manager.get_loaded_models()) if self.model_manager else 0
                    }
                    
                    await self.redis_client.setex(
                        heartbeat_key,
                        self.settings.heartbeat_interval * 2,  # TTL是心跳间隔的2倍
                        str(heartbeat_data)
                    )
                    
                    logger.debug(f"Heartbeat sent: {heartbeat_data}")
                
                await asyncio.sleep(self.settings.heartbeat_interval)
                
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                await asyncio.sleep(5)
    
    async def _metrics_loop(self):
        """指标更新循环"""
        while not self.shutdown_event.is_set():
            try:
                if self.model_manager:
                    # 更新模型指标
                    loaded_models_count = len(self.model_manager.get_loaded_models())
                    LOADED_MODELS.labels(worker_id=self.settings.worker_id).set(loaded_models_count)
                    
                    # 更新内存使用指标（如果可用）
                    try:
                        import psutil
                        process = psutil.Process(os.getpid())
                        memory_usage = process.memory_info().rss
                        MEMORY_USAGE.labels(worker_id=self.settings.worker_id).set(memory_usage)
                    except ImportError:
                        pass
                
                await asyncio.sleep(30)  # 每30秒更新一次指标
                
            except Exception as e:
                logger.error(f"Metrics update error: {e}")
                await asyncio.sleep(30)
    
    async def run(self):
        """运行工作节点服务"""
        try:
            # 初始化服务
            if not await self.initialize():
                logger.error("Service initialization failed")
                sys.exit(1)
            
            # 标记为运行状态
            self.is_running = True
            
            logger.info(f"Worker Node {self.settings.worker_id} is starting...")
            logger.info(f"Listening for tasks from Kafka topic: {self.settings.kafka_topic_tasks}")
            
            # 启动任务消费
            await self.task_consumer.start()
            
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Service error: {e}")
            sys.exit(1)
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """关闭服务"""
        if self.shutdown_event.is_set():
            return
        
        self.shutdown_event.set()
        self.is_running = False
        
        logger.info(f"Shutting down Worker Node {self.settings.worker_id}...")
        
        try:
            # 等待当前任务完成
            if self.active_tasks > 0:
                logger.info(f"Waiting for {self.active_tasks} active tasks to complete...")
                timeout = 30  # 30秒超时
                while self.active_tasks > 0 and timeout > 0:
                    await asyncio.sleep(1)
                    timeout -= 1
                
                if self.active_tasks > 0:
                    logger.warning(f"Shutdown with {self.active_tasks} tasks still active")
            
            # 停止任务消费
            if self.task_consumer:
                logger.info("Stopping task consumer...")
                await self.task_consumer.stop()
            
            # 关闭结果生产者
            if self.result_producer:
                logger.info("Closing result producer...")
                await self.result_producer.close()
            
            # 卸载所有模型
            if self.model_manager:
                logger.info("Unloading all models...")
                await self.model_manager.unload_all_models()
            
            # 关闭向量数据库连接
            if self.vector_db_manager:
                logger.info("Closing vector database connections...")
                await self.vector_db_manager.close_all_connections()
            
            # 停止后台任务
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                try:
                    await self.heartbeat_task
                except asyncio.CancelledError:
                    pass
            
            if self.metrics_task:
                self.metrics_task.cancel()
                try:
                    await self.metrics_task
                except asyncio.CancelledError:
                    pass
            
            # 发送最终心跳
            if self.redis_client:
                try:
                    heartbeat_key = f"worker:{self.settings.worker_id}:heartbeat"
                    await self.redis_client.setex(
                        heartbeat_key,
                        60,  # 1分钟TTL
                        str({
                            "timestamp": datetime.utcnow().isoformat(),
                            "status": "shutdown",
                            "active_tasks": 0
                        })
                    )
                    await self.redis_client.close()
                except Exception as e:
                    logger.error(f"Error sending final heartbeat: {e}")
            
            logger.info("Worker Node shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    async def health_check(self) -> bool:
        """健康检查"""
        try:
            # 检查Kafka连接
            if not self.kafka_consumer or not self.kafka_consumer.is_connected():
                logger.warning("Kafka connection unhealthy")
                return False
            
            # 检查Redis连接
            if self.redis_client:
                try:
                    await self.redis_client.ping()
                except Exception:
                    logger.warning("Redis connection unhealthy")
                    return False
            
            # 检查模型管理器
            if not self.model_manager:
                logger.warning("Model manager not available")
                return False
            
            # 检查向量数据库连接
            if self.vector_db_manager:
                db_info = await self.vector_db_manager.get_database_info()
                if not db_info.get('is_connected', False):
                    logger.warning("Vector database connection unhealthy")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def get_status(self) -> dict:
        """获取工作节点状态"""
        status = {
            "worker_id": self.settings.worker_id,
            "is_running": self.is_running,
            "active_tasks": self.active_tasks,
            "max_active_tasks": self.max_active_tasks,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if self.model_manager:
            status["loaded_models"] = self.model_manager.get_loaded_models()
            status["memory_usage"] = self.model_manager.get_memory_usage()
        
        if self.task_processor:
            status["processing_stats"] = self.task_processor.get_stats()
        
        return status


async def main():
    """主函数"""
    service = WorkerNodeService()
    
    try:
        await service.run()
    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)


def cli_main():
    """命令行入口点"""
    try:
        # 检查Python版本
        if sys.version_info < (3, 8):
            print("Python 3.8 or higher is required")
            sys.exit(1)
        
        # 运行服务
        asyncio.run(main())
        
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli_main()