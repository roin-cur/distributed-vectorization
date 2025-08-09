"""
公共模块
包含共享的数据模型、工具类和配置
"""
from .models import (
    VectorizationRequest,
    VectorizationTask,
    VectorizationResult,
    TaskStatus,
    TaskType,
    TaskMetrics,
    HealthStatus
)
from .kafka_producer import KafkaProducerManager

__all__ = [
    'VectorizationRequest',
    'VectorizationTask', 
    'VectorizationResult',
    'TaskStatus',
    'TaskType',
    'TaskMetrics',
    'HealthStatus',
    'KafkaProducerManager'
]

__version__ = "1.0.0"