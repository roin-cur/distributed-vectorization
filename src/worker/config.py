"""
工作节点配置管理
"""
import os
from typing import Dict, List, Optional, Any
from pydantic import BaseSettings, Field

from .model_manager import ModelConfig
from .vector_db_manager import VectorDBConfig


class WorkerSettings(BaseSettings):
    """工作节点配置设置"""
    
    # 基础配置
    worker_id: str = Field(default="worker-1", description="工作节点唯一标识")
    worker_name: str = Field(default="Vectorization Worker", description="工作节点名称")
    
    # Kafka配置
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka服务器地址"
    )
    kafka_group_id: str = Field(
        default="vectorization-workers",
        description="Kafka消费者组ID"
    )
    kafka_topic_tasks: str = Field(
        default="vectorization-tasks",
        description="向量化任务主题"
    )
    kafka_topic_results: str = Field(
        default="task-results",
        description="任务结果主题"
    )
    kafka_topic_status: str = Field(
        default="task-status",
        description="任务状态主题"
    )
    kafka_consumer_timeout: float = Field(
        default=1.0,
        description="Kafka消费者轮询超时时间(秒)"
    )
    kafka_max_poll_interval: int = Field(
        default=300000,
        description="Kafka最大轮询间隔(毫秒)"
    )
    
    # 模型配置
    models_cache_dir: str = Field(
        default="./models",
        description="模型缓存目录"
    )
    max_loaded_models: int = Field(
        default=3,
        description="最大同时加载的模型数量"
    )
    default_model: str = Field(
        default="sentence-transformers/all-MiniLM-L6-v2",
        description="默认向量化模型"
    )
    
    # 模型列表配置
    sentence_transformer_models: List[str] = Field(
        default=[
            "sentence-transformers/all-MiniLM-L6-v2",
            "sentence-transformers/all-mpnet-base-v2",
            "sentence-transformers/distilbert-base-nli-stsb-mean-tokens",
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
        ],
        description="Sentence Transformer模型列表"
    )
    
    huggingface_models: List[str] = Field(
        default=[
            "bert-base-uncased",
            "distilbert-base-uncased",
            "roberta-base"
        ],
        description="Hugging Face模型列表"
    )
    
    clip_models: List[str] = Field(
        default=[
            "ViT-B/32",
            "ViT-B/16",
            "ViT-L/14"
        ],
        description="CLIP模型列表"
    )
    
    # 设备配置
    device: str = Field(
        default="auto",
        description="计算设备 (auto, cpu, cuda, mps)"
    )
    gpu_memory_limit: Optional[float] = Field(
        default=None,
        description="GPU内存限制(GB)"
    )
    
    # 处理配置
    max_batch_size: int = Field(
        default=32,
        description="最大批处理大小"
    )
    default_batch_size: int = Field(
        default=16,
        description="默认批处理大小"
    )
    max_text_length: int = Field(
        default=512,
        description="最大文本长度"
    )
    document_chunk_size: int = Field(
        default=1000,
        description="文档分块大小"
    )
    
    # 向量数据库配置
    default_vector_db: str = Field(
        default="weaviate",
        description="默认向量数据库"
    )
    
    # Weaviate配置
    weaviate_url: str = Field(
        default="http://localhost:8080",
        description="Weaviate连接URL"
    )
    weaviate_api_key: Optional[str] = Field(
        default=None,
        description="Weaviate API密钥"
    )
    weaviate_timeout: int = Field(
        default=30,
        description="Weaviate连接超时时间(秒)"
    )
    weaviate_collection: str = Field(
        default="VectorizationResults",
        description="Weaviate集合名称"
    )
    
    # Qdrant配置
    qdrant_url: str = Field(
        default="localhost",
        description="Qdrant服务器地址"
    )
    qdrant_port: int = Field(
        default=6333,
        description="Qdrant服务器端口"
    )
    qdrant_api_key: Optional[str] = Field(
        default=None,
        description="Qdrant API密钥"
    )
    qdrant_collection: str = Field(
        default="vectorization_results",
        description="Qdrant集合名称"
    )
    
    # 监控配置
    enable_metrics: bool = Field(
        default=True,
        description="是否启用指标收集"
    )
    metrics_port: int = Field(
        default=9091,
        description="指标服务端口"
    )
    heartbeat_interval: int = Field(
        default=30,
        description="心跳间隔(秒)"
    )
    
    # 日志配置
    log_level: str = Field(
        default="INFO",
        description="日志级别"
    )
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="日志格式"
    )
    log_file: Optional[str] = Field(
        default=None,
        description="日志文件路径"
    )
    
    # 性能配置
    max_concurrent_tasks: int = Field(
        default=5,
        description="最大并发任务数"
    )
    task_timeout: int = Field(
        default=1800,
        description="任务超时时间(秒)"
    )
    memory_limit_mb: Optional[int] = Field(
        default=None,
        description="内存限制(MB)"
    )
    
    # Redis配置（用于心跳和状态）
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis连接URL"
    )
    redis_timeout: int = Field(
        default=10,
        description="Redis连接超时时间(秒)"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        env_prefix = "VECTORIZE_WORKER_"


def create_model_configs(settings: WorkerSettings) -> Dict[str, ModelConfig]:
    """
    根据设置创建模型配置
    
    Args:
        settings: 工作节点设置
        
    Returns:
        Dict[str, ModelConfig]: 模型配置字典
    """
    model_configs = {}
    
    # Sentence Transformer模型
    for model_name in settings.sentence_transformer_models:
        config = ModelConfig(
            name=model_name,
            model_type="sentence_transformer",
            model_path=model_name,
            max_length=settings.max_text_length,
            batch_size=settings.default_batch_size,
            device=settings.device,
            cache_dir=settings.models_cache_dir
        )
        model_configs[model_name] = config
    
    # Hugging Face模型
    for model_name in settings.huggingface_models:
        config = ModelConfig(
            name=model_name,
            model_type="huggingface",
            model_path=model_name,
            max_length=settings.max_text_length,
            batch_size=settings.default_batch_size,
            device=settings.device,
            cache_dir=settings.models_cache_dir
        )
        model_configs[model_name] = config
    
    # CLIP模型
    for model_name in settings.clip_models:
        config = ModelConfig(
            name=model_name,
            model_type="clip",
            model_path=model_name,
            batch_size=settings.default_batch_size,
            device=settings.device,
            cache_dir=settings.models_cache_dir
        )
        model_configs[model_name] = config
    
    return model_configs


def create_vector_db_configs(settings: WorkerSettings) -> Dict[str, VectorDBConfig]:
    """
    根据设置创建向量数据库配置
    
    Args:
        settings: 工作节点设置
        
    Returns:
        Dict[str, VectorDBConfig]: 向量数据库配置字典
    """
    db_configs = {}
    
    # Weaviate配置
    weaviate_params = {
        "url": settings.weaviate_url,
        "timeout": settings.weaviate_timeout
    }
    if settings.weaviate_api_key:
        weaviate_params["api_key"] = settings.weaviate_api_key
    
    db_configs["weaviate"] = VectorDBConfig(
        db_type="weaviate",
        connection_params=weaviate_params,
        collection_name=settings.weaviate_collection
    )
    
    # Qdrant配置
    qdrant_params = {
        "url": settings.qdrant_url,
        "port": settings.qdrant_port
    }
    if settings.qdrant_api_key:
        qdrant_params["api_key"] = settings.qdrant_api_key
    
    db_configs["qdrant"] = VectorDBConfig(
        db_type="qdrant",
        connection_params=qdrant_params,
        collection_name=settings.qdrant_collection
    )
    
    return db_configs


def validate_settings(settings: WorkerSettings) -> bool:
    """
    验证配置设置
    
    Args:
        settings: 工作节点设置
        
    Returns:
        bool: 验证是否通过
    """
    try:
        # 验证工作节点ID
        if not settings.worker_id:
            raise ValueError("Worker ID cannot be empty")
        
        # 验证端口范围
        if not (1 <= settings.metrics_port <= 65535):
            raise ValueError(f"Invalid metrics port: {settings.metrics_port}")
        
        if not (1 <= settings.qdrant_port <= 65535):
            raise ValueError(f"Invalid Qdrant port: {settings.qdrant_port}")
        
        # 验证批处理大小
        if settings.default_batch_size <= 0:
            raise ValueError(f"Invalid default batch size: {settings.default_batch_size}")
        
        if settings.max_batch_size < settings.default_batch_size:
            raise ValueError("Max batch size must be >= default batch size")
        
        # 验证超时设置
        if settings.task_timeout <= 0:
            raise ValueError(f"Invalid task timeout: {settings.task_timeout}")
        
        if settings.weaviate_timeout <= 0:
            raise ValueError(f"Invalid Weaviate timeout: {settings.weaviate_timeout}")
        
        if settings.redis_timeout <= 0:
            raise ValueError(f"Invalid Redis timeout: {settings.redis_timeout}")
        
        # 验证并发数
        if settings.max_concurrent_tasks <= 0:
            raise ValueError(f"Invalid max concurrent tasks: {settings.max_concurrent_tasks}")
        
        if settings.max_loaded_models <= 0:
            raise ValueError(f"Invalid max loaded models: {settings.max_loaded_models}")
        
        # 验证日志级别
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if settings.log_level.upper() not in valid_log_levels:
            raise ValueError(f"Invalid log level: {settings.log_level}")
        
        # 验证设备设置
        valid_devices = ["auto", "cpu", "cuda", "mps"]
        if settings.device not in valid_devices:
            raise ValueError(f"Invalid device: {settings.device}")
        
        # 验证模型列表
        if not settings.sentence_transformer_models:
            raise ValueError("At least one Sentence Transformer model must be configured")
        
        # 验证默认模型
        all_models = (
            settings.sentence_transformer_models +
            settings.huggingface_models +
            settings.clip_models
        )
        if settings.default_model not in all_models:
            raise ValueError(f"Default model {settings.default_model} not in configured models")
        
        # 验证向量数据库
        valid_dbs = ["weaviate", "qdrant"]
        if settings.default_vector_db not in valid_dbs:
            raise ValueError(f"Invalid default vector database: {settings.default_vector_db}")
        
        return True
        
    except Exception as e:
        print(f"Configuration validation failed: {e}")
        return False


def print_settings(settings: WorkerSettings):
    """打印当前配置（隐藏敏感信息）"""
    print("=== Worker Node Configuration ===")
    print(f"Worker ID: {settings.worker_id}")
    print(f"Kafka: {settings.kafka_bootstrap_servers}")
    print(f"Default Model: {settings.default_model}")
    print(f"Device: {settings.device}")
    print(f"Max Loaded Models: {settings.max_loaded_models}")
    print(f"Default Vector DB: {settings.default_vector_db}")
    print(f"Weaviate: {settings.weaviate_url}")
    print(f"Max Concurrent Tasks: {settings.max_concurrent_tasks}")
    print(f"Log Level: {settings.log_level}")
    print("================================")


def get_settings() -> WorkerSettings:
    """获取配置实例"""
    return WorkerSettings()


# 创建全局配置实例
settings = WorkerSettings()