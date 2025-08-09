"""
公共数据模型定义
"""
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
import uuid


class TaskStatus(str, Enum):
    """任务状态枚举"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskType(str, Enum):
    """任务类型枚举"""
    TEXT_VECTORIZATION = "text_vectorization"
    IMAGE_VECTORIZATION = "image_vectorization"
    DOCUMENT_VECTORIZATION = "document_vectorization"
    BATCH_VECTORIZATION = "batch_vectorization"


class VectorizationRequest(BaseModel):
    """向量化请求模型"""
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="任务唯一标识")
    task_type: TaskType = Field(..., description="任务类型")
    data: Union[str, List[str], Dict[str, Any]] = Field(..., description="待向量化的数据")
    model_name: Optional[str] = Field(default="sentence-transformers/all-MiniLM-L6-v2", description="使用的模型名称")
    batch_size: Optional[int] = Field(default=32, description="批处理大小")
    priority: Optional[int] = Field(default=1, description="任务优先级，数字越大优先级越高")
    callback_url: Optional[str] = Field(default=None, description="任务完成后的回调URL")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="额外元数据")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class VectorizationTask(BaseModel):
    """向量化任务模型"""
    task_id: str = Field(..., description="任务唯一标识")
    task_type: TaskType = Field(..., description="任务类型")
    status: TaskStatus = Field(default=TaskStatus.PENDING, description="任务状态")
    data: Union[str, List[str], Dict[str, Any]] = Field(..., description="待向量化的数据")
    model_name: str = Field(..., description="使用的模型名称")
    batch_size: int = Field(default=32, description="批处理大小")
    priority: int = Field(default=1, description="任务优先级")
    callback_url: Optional[str] = Field(default=None, description="任务完成后的回调URL")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="额外元数据")
    
    # 时间戳
    created_at: datetime = Field(default_factory=datetime.utcnow, description="创建时间")
    started_at: Optional[datetime] = Field(default=None, description="开始处理时间")
    completed_at: Optional[datetime] = Field(default=None, description="完成时间")
    
    # 处理信息
    worker_id: Optional[str] = Field(default=None, description="处理该任务的工作节点ID")
    progress: float = Field(default=0.0, description="任务进度 (0.0-1.0)")
    error_message: Optional[str] = Field(default=None, description="错误信息")
    retry_count: int = Field(default=0, description="重试次数")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class VectorizationResult(BaseModel):
    """向量化结果模型"""
    task_id: str = Field(..., description="任务唯一标识")
    status: TaskStatus = Field(..., description="任务状态")
    vectors: Optional[List[List[float]]] = Field(default=None, description="向量结果")
    embeddings_count: Optional[int] = Field(default=None, description="向量数量")
    processing_time: Optional[float] = Field(default=None, description="处理时间(秒)")
    model_used: Optional[str] = Field(default=None, description="实际使用的模型")
    error_message: Optional[str] = Field(default=None, description="错误信息")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="额外元数据")
    completed_at: datetime = Field(default_factory=datetime.utcnow, description="完成时间")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class TaskMetrics(BaseModel):
    """任务指标模型"""
    total_tasks: int = Field(default=0, description="总任务数")
    pending_tasks: int = Field(default=0, description="待处理任务数")
    processing_tasks: int = Field(default=0, description="处理中任务数")
    completed_tasks: int = Field(default=0, description="已完成任务数")
    failed_tasks: int = Field(default=0, description="失败任务数")
    average_processing_time: float = Field(default=0.0, description="平均处理时间")
    total_vectors_generated: int = Field(default=0, description="总生成向量数")


class HealthStatus(BaseModel):
    """健康状态模型"""
    status: str = Field(..., description="服务状态")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="检查时间")
    kafka_connected: bool = Field(default=False, description="Kafka连接状态")
    weaviate_connected: bool = Field(default=False, description="Weaviate连接状态")
    redis_connected: bool = Field(default=False, description="Redis连接状态")
    active_workers: int = Field(default=0, description="活跃工作节点数")
    queue_size: int = Field(default=0, description="队列大小")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }