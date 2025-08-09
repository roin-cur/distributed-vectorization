"""
Kafka消费者模块
用于从Kafka队列消费向量化任务
"""
import json
import logging
import asyncio
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient

from .models import VectorizationTask, TaskStatus

logger = logging.getLogger(__name__)


class KafkaConsumerManager:
    """Kafka消费者管理器"""
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "vectorization-workers",
        worker_id: str = "worker-1"
    ):
        """
        初始化Kafka消费者
        
        Args:
            bootstrap_servers: Kafka服务器地址
            group_id: 消费者组ID
            worker_id: 工作节点ID
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.worker_id = worker_id
        
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'client.id': worker_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # 手动提交偏移量
            'max.poll.interval.ms': 300000,  # 5分钟
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 3000,
            'fetch.min.bytes': 1,
            'fetch.max.wait.ms': 500,
            'max.partition.fetch.bytes': 1048576,  # 1MB
            'isolation.level': 'read_committed',
        }
        
        self.consumer: Optional[Consumer] = None
        self.admin_client: Optional[AdminClient] = None
        self.is_running = False
        self.message_handlers: Dict[str, Callable] = {}
        
        # 主题配置
        self.topics = {
            'vectorization_tasks': 'vectorization-tasks',
            'task_results': 'task-results',
            'task_status': 'task-status'
        }
        
        # 统计信息
        self.stats = {
            'messages_consumed': 0,
            'messages_processed': 0,
            'messages_failed': 0,
            'last_message_time': None
        }
    
    async def initialize(self) -> bool:
        """
        初始化Kafka消费者
        
        Returns:
            bool: 初始化是否成功
        """
        try:
            # 创建消费者
            self.consumer = Consumer(self.consumer_config)
            
            # 创建管理客户端
            admin_config = {'bootstrap.servers': self.bootstrap_servers}
            self.admin_client = AdminClient(admin_config)
            
            # 检查主题是否存在
            await self._check_topics()
            
            logger.info(f"Kafka consumer initialized successfully for worker {self.worker_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            return False
    
    async def _check_topics(self):
        """检查主题是否存在"""
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            existing_topics = set(metadata.topics.keys())
            
            required_topics = set(self.topics.values())
            missing_topics = required_topics - existing_topics
            
            if missing_topics:
                logger.warning(f"Missing topics: {missing_topics}")
                # 在生产环境中，应该等待主题被创建或抛出异常
            else:
                logger.info("All required topics exist")
                
        except Exception as e:
            logger.error(f"Error checking topics: {e}")
            raise
    
    def register_handler(self, topic: str, handler: Callable[[Dict[str, Any]], None]):
        """
        注册消息处理器
        
        Args:
            topic: 主题名称
            handler: 处理函数
        """
        self.message_handlers[topic] = handler
        logger.info(f"Registered handler for topic: {topic}")
    
    async def start_consuming(self, topics: Optional[List[str]] = None):
        """
        开始消费消息
        
        Args:
            topics: 要订阅的主题列表，默认为向量化任务主题
        """
        if not self.consumer:
            raise RuntimeError("Consumer not initialized")
        
        if topics is None:
            topics = [self.topics['vectorization_tasks']]
        
        try:
            # 订阅主题
            self.consumer.subscribe(topics)
            self.is_running = True
            
            logger.info(f"Started consuming from topics: {topics}")
            
            # 消费循环
            while self.is_running:
                try:
                    # 轮询消息
                    msg = self.consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # 到达分区末尾，继续
                            continue
                        else:
                            logger.error(f"Consumer error: {msg.error()}")
                            continue
                    
                    # 处理消息
                    await self._process_message(msg)
                    
                except Exception as e:
                    logger.error(f"Error in consumer loop: {e}")
                    await asyncio.sleep(1)
            
        except Exception as e:
            logger.error(f"Failed to start consuming: {e}")
            raise
        finally:
            if self.consumer:
                self.consumer.close()
    
    async def _process_message(self, msg):
        """处理单个消息"""
        try:
            # 更新统计信息
            self.stats['messages_consumed'] += 1
            self.stats['last_message_time'] = datetime.utcnow()
            
            # 解析消息
            topic = msg.topic()
            key = msg.key().decode('utf-8') if msg.key() else None
            value = msg.value().decode('utf-8')
            headers = dict(msg.headers()) if msg.headers() else {}
            
            logger.debug(f"Received message from topic {topic}, key: {key}")
            
            # 解析JSON数据
            try:
                message_data = json.loads(value)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message JSON: {e}")
                self.stats['messages_failed'] += 1
                # 提交偏移量以跳过这个错误消息
                self.consumer.commit(msg)
                return
            
            # 调用相应的处理器
            if topic in self.message_handlers:
                handler = self.message_handlers[topic]
                
                # 在线程池中执行处理器以避免阻塞
                loop = asyncio.get_event_loop()
                
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(message_data, headers, key)
                    else:
                        await loop.run_in_executor(None, handler, message_data, headers, key)
                    
                    # 处理成功，提交偏移量
                    self.consumer.commit(msg)
                    self.stats['messages_processed'] += 1
                    
                    logger.debug(f"Message processed successfully: {key}")
                    
                except Exception as e:
                    logger.error(f"Handler failed for message {key}: {e}")
                    self.stats['messages_failed'] += 1
                    
                    # 根据错误类型决定是否提交偏移量
                    # 对于可重试的错误，可以选择不提交偏移量
                    if self._should_retry_message(e):
                        logger.info(f"Message {key} will be retried")
                        # 不提交偏移量，消息会被重新消费
                        await asyncio.sleep(5)  # 等待一段时间再重试
                    else:
                        logger.warning(f"Skipping message {key} due to non-retryable error")
                        self.consumer.commit(msg)
            else:
                logger.warning(f"No handler registered for topic: {topic}")
                # 提交偏移量以跳过未处理的消息
                self.consumer.commit(msg)
                
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}")
            self.stats['messages_failed'] += 1
    
    def _should_retry_message(self, error: Exception) -> bool:
        """
        判断是否应该重试消息
        
        Args:
            error: 处理消息时发生的错误
            
        Returns:
            bool: 是否应该重试
        """
        # 定义可重试的错误类型
        retryable_errors = (
            ConnectionError,
            TimeoutError,
            # 可以添加更多可重试的错误类型
        )
        
        # 对于模型加载错误、数据格式错误等，通常不应该重试
        non_retryable_errors = (
            ValueError,
            TypeError,
            KeyError,
            json.JSONDecodeError,
        )
        
        if isinstance(error, non_retryable_errors):
            return False
        
        if isinstance(error, retryable_errors):
            return True
        
        # 默认不重试未知错误
        return False
    
    async def stop_consuming(self):
        """停止消费消息"""
        logger.info("Stopping message consumption...")
        self.is_running = False
        
        # 等待当前消息处理完成
        await asyncio.sleep(1)
        
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer closed")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取消费者统计信息"""
        return {
            **self.stats,
            'is_running': self.is_running,
            'worker_id': self.worker_id,
            'group_id': self.group_id
        }
    
    def is_connected(self) -> bool:
        """检查消费者连接状态"""
        try:
            if not self.consumer:
                return False
            
            # 尝试获取元数据来检查连接
            metadata = self.consumer.list_topics(timeout=5)
            return len(metadata.topics) >= 0
            
        except Exception:
            return False


class TaskConsumer:
    """向量化任务消费者"""
    
    def __init__(
        self,
        kafka_manager: KafkaConsumerManager,
        task_processor: Callable[[VectorizationTask], None]
    ):
        """
        初始化任务消费者
        
        Args:
            kafka_manager: Kafka消费者管理器
            task_processor: 任务处理器函数
        """
        self.kafka_manager = kafka_manager
        self.task_processor = task_processor
        
        # 注册消息处理器
        self.kafka_manager.register_handler(
            self.kafka_manager.topics['vectorization_tasks'],
            self._handle_vectorization_task
        )
    
    async def _handle_vectorization_task(
        self,
        message_data: Dict[str, Any],
        headers: Dict[str, Any],
        key: Optional[str]
    ):
        """
        处理向量化任务消息
        
        Args:
            message_data: 消息数据
            headers: 消息头
            key: 消息键
        """
        try:
            # 解析任务数据
            task = VectorizationTask.model_validate(message_data)
            
            logger.info(f"Received vectorization task: {task.task_id}")
            logger.debug(f"Task details: type={task.task_type}, model={task.model_name}")
            
            # 检查任务是否已被取消
            if task.status == TaskStatus.CANCELLED:
                logger.info(f"Task {task.task_id} is cancelled, skipping")
                return
            
            # 调用任务处理器
            if asyncio.iscoroutinefunction(self.task_processor):
                await self.task_processor(task)
            else:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.task_processor, task)
            
            logger.info(f"Task {task.task_id} processed successfully")
            
        except Exception as e:
            logger.error(f"Failed to handle vectorization task: {e}")
            raise
    
    async def start(self):
        """开始消费任务"""
        logger.info("Starting task consumer...")
        await self.kafka_manager.start_consuming()
    
    async def stop(self):
        """停止消费任务"""
        logger.info("Stopping task consumer...")
        await self.kafka_manager.stop_consuming()


class ResultProducer:
    """结果生产者（用于发送处理结果）"""
    
    def __init__(self, kafka_manager: KafkaConsumerManager):
        """
        初始化结果生产者
        
        Args:
            kafka_manager: Kafka管理器（复用连接配置）
        """
        from .kafka_producer import KafkaProducerManager
        
        self.producer = KafkaProducerManager(kafka_manager.bootstrap_servers)
    
    async def initialize(self) -> bool:
        """初始化生产者"""
        return await self.producer.initialize()
    
    async def send_task_result(
        self,
        task_id: str,
        result_data: Dict[str, Any]
    ) -> bool:
        """
        发送任务结果
        
        Args:
            task_id: 任务ID
            result_data: 结果数据
            
        Returns:
            bool: 发送是否成功
        """
        try:
            message_value = json.dumps(result_data, default=str, ensure_ascii=False)
            
            # 发送到结果主题
            success = await self.producer.producer.produce(
                topic=self.producer.topics['task_results'],
                key=task_id,
                value=message_value,
                callback=self.producer._delivery_callback
            )
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to send task result: {e}")
            return False
    
    async def send_status_update(
        self,
        task_id: str,
        status: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """发送状态更新"""
        return await self.producer.send_task_status_update(task_id, status, metadata)
    
    async def close(self):
        """关闭生产者"""
        await self.producer.close()