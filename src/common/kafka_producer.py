"""
Kafka生产者模块
用于将向量化任务发送到Kafka队列
"""
import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException

from .models import VectorizationTask

logger = logging.getLogger(__name__)


class KafkaProducerManager:
    """Kafka生产者管理器"""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """
        初始化Kafka生产者
        
        Args:
            bootstrap_servers: Kafka服务器地址
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'vectorization-master',
            'acks': 'all',  # 等待所有副本确认
            'retries': 3,
            'retry.backoff.ms': 100,
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 60000,
            'batch.size': 16384,
            'linger.ms': 5,
            'compression.type': 'snappy',
            'max.in.flight.requests.per.connection': 1,  # 保证消息顺序
        }
        
        self.producer: Optional[Producer] = None
        self.admin_client: Optional[AdminClient] = None
        
        # 主题配置
        self.topics = {
            'vectorization_tasks': 'vectorization-tasks',
            'task_results': 'task-results',
            'task_status': 'task-status'
        }
    
    async def initialize(self) -> bool:
        """
        初始化Kafka连接和主题
        
        Returns:
            bool: 初始化是否成功
        """
        try:
            # 创建生产者
            self.producer = Producer(self.producer_config)
            
            # 创建管理客户端
            admin_config = {'bootstrap.servers': self.bootstrap_servers}
            self.admin_client = AdminClient(admin_config)
            
            # 创建主题
            await self._create_topics()
            
            logger.info(f"Kafka producer initialized successfully, connected to {self.bootstrap_servers}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            return False
    
    async def _create_topics(self):
        """创建必要的Kafka主题"""
        try:
            # 检查现有主题
            metadata = self.admin_client.list_topics(timeout=10)
            existing_topics = set(metadata.topics.keys())
            
            # 定义要创建的主题
            topics_to_create = []
            topic_configs = {
                self.topics['vectorization_tasks']: {
                    'num_partitions': 6,
                    'replication_factor': 1,
                    'config': {
                        'retention.ms': '604800000',  # 7天
                        'cleanup.policy': 'delete'
                    }
                },
                self.topics['task_results']: {
                    'num_partitions': 3,
                    'replication_factor': 1,
                    'config': {
                        'retention.ms': '2592000000',  # 30天
                        'cleanup.policy': 'delete'
                    }
                },
                self.topics['task_status']: {
                    'num_partitions': 3,
                    'replication_factor': 1,
                    'config': {
                        'retention.ms': '86400000',  # 1天
                        'cleanup.policy': 'delete'
                    }
                }
            }
            
            for topic_name, config in topic_configs.items():
                if topic_name not in existing_topics:
                    topic = NewTopic(
                        topic=topic_name,
                        num_partitions=config['num_partitions'],
                        replication_factor=config['replication_factor'],
                        config=config['config']
                    )
                    topics_to_create.append(topic)
            
            if topics_to_create:
                # 创建主题
                futures = self.admin_client.create_topics(topics_to_create)
                
                # 等待创建完成
                for topic_name, future in futures.items():
                    try:
                        future.result()  # 阻塞直到完成
                        logger.info(f"Topic '{topic_name}' created successfully")
                    except Exception as e:
                        logger.error(f"Failed to create topic '{topic_name}': {e}")
            else:
                logger.info("All required topics already exist")
                
        except Exception as e:
            logger.error(f"Error creating topics: {e}")
            raise
    
    def _delivery_callback(self, err, msg):
        """消息投递回调函数"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    async def send_vectorization_task(self, task: VectorizationTask) -> bool:
        """
        发送向量化任务到Kafka
        
        Args:
            task: 向量化任务对象
            
        Returns:
            bool: 发送是否成功
        """
        try:
            if not self.producer:
                logger.error("Kafka producer not initialized")
                return False
            
            # 序列化任务数据
            task_data = task.model_dump()
            message_value = json.dumps(task_data, default=str, ensure_ascii=False)
            
            # 生成消息键（用于分区）
            message_key = f"{task.task_type}:{task.priority}"
            
            # 发送消息
            self.producer.produce(
                topic=self.topics['vectorization_tasks'],
                key=message_key,
                value=message_value,
                callback=self._delivery_callback,
                headers={
                    'task_id': task.task_id,
                    'task_type': task.task_type.value,
                    'priority': str(task.priority),
                    'created_at': task.created_at.isoformat()
                }
            )
            
            # 刷新缓冲区（非阻塞）
            self.producer.poll(0)
            
            logger.info(f"Vectorization task {task.task_id} sent to Kafka successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send vectorization task {task.task_id} to Kafka: {e}")
            return False
    
    async def send_task_status_update(self, task_id: str, status: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        发送任务状态更新
        
        Args:
            task_id: 任务ID
            status: 新状态
            metadata: 额外元数据
            
        Returns:
            bool: 发送是否成功
        """
        try:
            if not self.producer:
                logger.error("Kafka producer not initialized")
                return False
            
            status_data = {
                'task_id': task_id,
                'status': status,
                'timestamp': datetime.utcnow().isoformat(),
                'metadata': metadata or {}
            }
            
            message_value = json.dumps(status_data, ensure_ascii=False)
            
            self.producer.produce(
                topic=self.topics['task_status'],
                key=task_id,
                value=message_value,
                callback=self._delivery_callback
            )
            
            self.producer.poll(0)
            
            logger.debug(f"Task status update for {task_id} sent to Kafka")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send task status update for {task_id}: {e}")
            return False
    
    async def flush(self, timeout: float = 10.0) -> bool:
        """
        刷新所有待发送的消息
        
        Args:
            timeout: 超时时间（秒）
            
        Returns:
            bool: 是否成功刷新所有消息
        """
        try:
            if not self.producer:
                return True
            
            remaining = self.producer.flush(timeout)
            if remaining > 0:
                logger.warning(f"{remaining} messages failed to flush within {timeout} seconds")
                return False
            
            logger.debug("All messages flushed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error flushing messages: {e}")
            return False
    
    async def close(self):
        """关闭Kafka生产者"""
        try:
            if self.producer:
                await self.flush()
                self.producer = None
                logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
    
    def is_connected(self) -> bool:
        """检查Kafka连接状态"""
        try:
            if not self.producer:
                return False
            
            # 尝试获取元数据来检查连接
            metadata = self.producer.list_topics(timeout=5)
            return len(metadata.topics) >= 0
            
        except Exception:
            return False