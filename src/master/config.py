"""
主节点配置管理
"""
import os
from typing import Optional, List
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """主节点配置设置"""
    
    # 服务配置
    host: str = Field(default="0.0.0.0", description="API服务监听地址")
    port: int = Field(default=8000, description="API服务端口")
    workers: int = Field(default=1, description="API服务工作进程数")
    
    # Kafka配置
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092", 
        description="Kafka服务器地址"
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
    
    # Weaviate配置
    weaviate_url: str = Field(
        default="http://localhost:8080",
        description="Weaviate数据库URL"
    )
    weaviate_timeout: int = Field(
        default=30,
        description="Weaviate连接超时时间(秒)"
    )
    
    # Redis配置
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis连接URL"
    )
    redis_timeout: int = Field(
        default=10,
        description="Redis连接超时时间(秒)"
    )
    redis_max_connections: int = Field(
        default=20,
        description="Redis最大连接数"
    )
    
    # 任务配置
    max_task_retry: int = Field(
        default=3,
        description="任务最大重试次数"
    )
    task_timeout: int = Field(
        default=3600,
        description="任务超时时间(秒)"
    )
    default_batch_size: int = Field(
        default=32,
        description="默认批处理大小"
    )
    max_batch_size: int = Field(
        default=512,
        description="最大批处理大小"
    )
    
    # 模型配置
    default_model: str = Field(
        default="sentence-transformers/all-MiniLM-L6-v2",
        description="默认向量化模型"
    )
    supported_models: List[str] = Field(
        default=[
            "sentence-transformers/all-MiniLM-L6-v2",
            "sentence-transformers/all-mpnet-base-v2",
            "sentence-transformers/distilbert-base-nli-stsb-mean-tokens",
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
        ],
        description="支持的模型列表"
    )
    
    # 监控配置
    metrics_enabled: bool = Field(
        default=True,
        description="是否启用指标收集"
    )
    metrics_port: int = Field(
        default=9090,
        description="指标服务端口"
    )
    health_check_interval: int = Field(
        default=30,
        description="健康检查间隔(秒)"
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
    
    # 安全配置
    api_key: Optional[str] = Field(
        default=None,
        description="API密钥"
    )
    cors_origins: List[str] = Field(
        default=["*"],
        description="CORS允许的源"
    )
    rate_limit_requests: int = Field(
        default=100,
        description="速率限制：每分钟请求数"
    )
    
    # 性能配置
    max_concurrent_tasks: int = Field(
        default=1000,
        description="最大并发任务数"
    )
    task_queue_size: int = Field(
        default=10000,
        description="任务队列大小"
    )
    result_cache_ttl: int = Field(
        default=3600,
        description="结果缓存TTL(秒)"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        
        # 环境变量前缀
        env_prefix = "VECTORIZE_MASTER_"


# 创建全局配置实例
settings = Settings()


def get_settings() -> Settings:
    """获取配置实例"""
    return settings


def validate_settings() -> bool:
    """验证配置设置"""
    try:
        # 验证端口范围
        if not (1 <= settings.port <= 65535):
            raise ValueError(f"Invalid port: {settings.port}")
        
        if not (1 <= settings.metrics_port <= 65535):
            raise ValueError(f"Invalid metrics port: {settings.metrics_port}")
        
        # 验证批处理大小
        if settings.default_batch_size <= 0:
            raise ValueError(f"Invalid default batch size: {settings.default_batch_size}")
        
        if settings.max_batch_size < settings.default_batch_size:
            raise ValueError("Max batch size must be >= default batch size")
        
        # 验证超时设置
        if settings.task_timeout <= 0:
            raise ValueError(f"Invalid task timeout: {settings.task_timeout}")
        
        if settings.redis_timeout <= 0:
            raise ValueError(f"Invalid Redis timeout: {settings.redis_timeout}")
        
        if settings.weaviate_timeout <= 0:
            raise ValueError(f"Invalid Weaviate timeout: {settings.weaviate_timeout}")
        
        # 验证重试次数
        if settings.max_task_retry < 0:
            raise ValueError(f"Invalid max retry count: {settings.max_task_retry}")
        
        # 验证日志级别
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if settings.log_level.upper() not in valid_log_levels:
            raise ValueError(f"Invalid log level: {settings.log_level}")
        
        return True
        
    except Exception as e:
        print(f"Configuration validation failed: {e}")
        return False


def print_settings():
    """打印当前配置（隐藏敏感信息）"""
    print("=== Master Node Configuration ===")
    print(f"Service: {settings.host}:{settings.port}")
    print(f"Kafka: {settings.kafka_bootstrap_servers}")
    print(f"Weaviate: {settings.weaviate_url}")
    print(f"Redis: {settings.redis_url}")
    print(f"Log Level: {settings.log_level}")
    print(f"Default Model: {settings.default_model}")
    print(f"Max Concurrent Tasks: {settings.max_concurrent_tasks}")
    print(f"Task Timeout: {settings.task_timeout}s")
    print("=================================")