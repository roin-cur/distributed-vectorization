"""
工作节点模块
负责消费Kafka任务、执行向量化处理并存储结果
"""
from .main import WorkerNodeService, main, cli_main
from .config import WorkerSettings, get_settings
from .model_manager import ModelManager, ModelConfig
from .vector_db_manager import VectorDBManager, VectorDBConfig
from .task_processor import TaskProcessor

__all__ = [
    'WorkerNodeService',
    'main',
    'cli_main',
    'WorkerSettings',
    'get_settings',
    'ModelManager',
    'ModelConfig',
    'VectorDBManager',
    'VectorDBConfig',
    'TaskProcessor'
]

__version__ = "1.0.0"