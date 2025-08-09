"""
主节点API服务
提供向量化任务提交和管理的REST API接口
"""
import logging
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError

from ..common.models import (
    VectorizationRequest, 
    VectorizationTask, 
    VectorizationResult,
    TaskStatus,
    TaskType,
    TaskMetrics,
    HealthStatus
)
from ..common.kafka_producer import KafkaProducerManager
from .task_manager import TaskManager
from .config import Settings

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 全局变量
kafka_producer: Optional[KafkaProducerManager] = None
task_manager: Optional[TaskManager] = None
settings = Settings()


class TaskSubmissionResponse(BaseModel):
    """任务提交响应模型"""
    task_id: str
    status: str
    message: str
    estimated_processing_time: Optional[float] = None


class TaskListResponse(BaseModel):
    """任务列表响应模型"""
    tasks: List[VectorizationTask]
    total: int
    page: int
    page_size: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global kafka_producer, task_manager
    
    # 启动时初始化
    logger.info("Initializing Master Node API...")
    
    try:
        # 初始化Kafka生产者
        kafka_producer = KafkaProducerManager(settings.kafka_bootstrap_servers)
        kafka_init_success = await kafka_producer.initialize()
        
        if not kafka_init_success:
            logger.error("Failed to initialize Kafka producer")
            raise RuntimeError("Kafka initialization failed")
        
        # 初始化任务管理器
        task_manager = TaskManager(kafka_producer, settings)
        await task_manager.initialize()
        
        logger.info("Master Node API initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize Master Node API: {e}")
        raise
    
    yield
    
    # 关闭时清理
    logger.info("Shutting down Master Node API...")
    
    if kafka_producer:
        await kafka_producer.close()
    
    if task_manager:
        await task_manager.close()
    
    logger.info("Master Node API shutdown complete")


# 创建FastAPI应用
app = FastAPI(
    title="分布式向量化系统 - 主节点API",
    description="提供向量化任务提交和管理的REST API接口",
    version="1.0.0",
    lifespan=lifespan
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境中应该限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_kafka_producer() -> KafkaProducerManager:
    """获取Kafka生产者依赖"""
    if kafka_producer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka producer not available"
        )
    return kafka_producer


def get_task_manager() -> TaskManager:
    """获取任务管理器依赖"""
    if task_manager is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Task manager not available"
        )
    return task_manager


@app.get("/", summary="根路径", tags=["基础"])
async def root():
    """根路径响应"""
    return {
        "service": "分布式向量化系统 - 主节点",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/health", response_model=HealthStatus, summary="健康检查", tags=["监控"])
async def health_check(
    task_mgr: TaskManager = Depends(get_task_manager),
    producer: KafkaProducerManager = Depends(get_kafka_producer)
):
    """
    健康检查接口
    检查各个组件的连接状态和系统健康度
    """
    try:
        # 检查各组件连接状态
        kafka_connected = producer.is_connected()
        weaviate_connected = await task_mgr.check_weaviate_connection()
        redis_connected = await task_mgr.check_redis_connection()
        
        # 获取系统状态
        queue_size = await task_mgr.get_queue_size()
        active_workers = await task_mgr.get_active_workers_count()
        
        # 确定整体状态
        overall_status = "healthy"
        if not kafka_connected or not weaviate_connected:
            overall_status = "degraded"
        if not kafka_connected and not weaviate_connected:
            overall_status = "unhealthy"
        
        return HealthStatus(
            status=overall_status,
            kafka_connected=kafka_connected,
            weaviate_connected=weaviate_connected,
            redis_connected=redis_connected,
            active_workers=active_workers,
            queue_size=queue_size
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Health check failed: {str(e)}"
        )


@app.post("/tasks/submit", response_model=TaskSubmissionResponse, summary="提交向量化任务", tags=["任务管理"])
async def submit_vectorization_task(
    request: VectorizationRequest,
    background_tasks: BackgroundTasks,
    task_mgr: TaskManager = Depends(get_task_manager),
    producer: KafkaProducerManager = Depends(get_kafka_producer)
):
    """
    提交向量化任务
    
    接受客户端的向量化请求，创建任务并发送到Kafka队列
    """
    try:
        # 验证请求数据
        if not request.data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Data field cannot be empty"
            )
        
        # 创建向量化任务
        task = VectorizationTask(
            task_id=request.task_id,
            task_type=request.task_type,
            data=request.data,
            model_name=request.model_name,
            batch_size=request.batch_size,
            priority=request.priority,
            callback_url=request.callback_url,
            metadata=request.metadata
        )
        
        # 保存任务到数据库/缓存
        await task_mgr.save_task(task)
        
        # 发送任务到Kafka
        success = await producer.send_vectorization_task(task)
        
        if not success:
            # 如果发送失败，更新任务状态
            await task_mgr.update_task_status(task.task_id, TaskStatus.FAILED, "Failed to send task to queue")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to submit task to processing queue"
            )
        
        # 更新任务状态为已提交
        await producer.send_task_status_update(task.task_id, TaskStatus.PENDING.value)
        
        # 估算处理时间（基于队列长度和数据大小）
        estimated_time = await task_mgr.estimate_processing_time(task)
        
        logger.info(f"Task {task.task_id} submitted successfully")
        
        return TaskSubmissionResponse(
            task_id=task.task_id,
            status=TaskStatus.PENDING.value,
            message="Task submitted successfully",
            estimated_processing_time=estimated_time
        )
        
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Validation error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to submit task: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@app.get("/tasks/{task_id}", response_model=VectorizationTask, summary="获取任务详情", tags=["任务管理"])
async def get_task(
    task_id: str,
    task_mgr: TaskManager = Depends(get_task_manager)
):
    """获取指定任务的详细信息"""
    try:
        task = await task_mgr.get_task(task_id)
        
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found"
            )
        
        return task
        
    except Exception as e:
        logger.error(f"Failed to get task {task_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve task: {str(e)}"
        )


@app.get("/tasks", response_model=TaskListResponse, summary="获取任务列表", tags=["任务管理"])
async def list_tasks(
    status_filter: Optional[TaskStatus] = None,
    task_type_filter: Optional[TaskType] = None,
    page: int = 1,
    page_size: int = 20,
    task_mgr: TaskManager = Depends(get_task_manager)
):
    """
    获取任务列表
    支持按状态和类型过滤，分页查询
    """
    try:
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 100:
            page_size = 20
        
        tasks, total = await task_mgr.list_tasks(
            status_filter=status_filter,
            task_type_filter=task_type_filter,
            page=page,
            page_size=page_size
        )
        
        return TaskListResponse(
            tasks=tasks,
            total=total,
            page=page,
            page_size=page_size
        )
        
    except Exception as e:
        logger.error(f"Failed to list tasks: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve tasks: {str(e)}"
        )


@app.delete("/tasks/{task_id}", summary="取消任务", tags=["任务管理"])
async def cancel_task(
    task_id: str,
    task_mgr: TaskManager = Depends(get_task_manager),
    producer: KafkaProducerManager = Depends(get_kafka_producer)
):
    """取消指定的任务"""
    try:
        task = await task_mgr.get_task(task_id)
        
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found"
            )
        
        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot cancel task in {task.status.value} status"
            )
        
        # 更新任务状态
        await task_mgr.update_task_status(task_id, TaskStatus.CANCELLED, "Task cancelled by user")
        await producer.send_task_status_update(task_id, TaskStatus.CANCELLED.value)
        
        logger.info(f"Task {task_id} cancelled successfully")
        
        return {"message": f"Task {task_id} cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel task {task_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel task: {str(e)}"
        )


@app.get("/metrics", response_model=TaskMetrics, summary="获取系统指标", tags=["监控"])
async def get_metrics(task_mgr: TaskManager = Depends(get_task_manager)):
    """获取系统运行指标"""
    try:
        metrics = await task_mgr.get_metrics()
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve metrics: {str(e)}"
        )


@app.post("/tasks/batch", summary="批量提交任务", tags=["任务管理"])
async def submit_batch_tasks(
    requests: List[VectorizationRequest],
    background_tasks: BackgroundTasks,
    task_mgr: TaskManager = Depends(get_task_manager),
    producer: KafkaProducerManager = Depends(get_kafka_producer)
):
    """批量提交多个向量化任务"""
    try:
        if len(requests) > 100:  # 限制批量大小
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch size cannot exceed 100 tasks"
            )
        
        results = []
        
        for request in requests:
            try:
                # 创建任务
                task = VectorizationTask(
                    task_id=request.task_id,
                    task_type=request.task_type,
                    data=request.data,
                    model_name=request.model_name,
                    batch_size=request.batch_size,
                    priority=request.priority,
                    callback_url=request.callback_url,
                    metadata=request.metadata
                )
                
                # 保存和发送任务
                await task_mgr.save_task(task)
                success = await producer.send_vectorization_task(task)
                
                if success:
                    await producer.send_task_status_update(task.task_id, TaskStatus.PENDING.value)
                    results.append({
                        "task_id": task.task_id,
                        "status": "submitted",
                        "message": "Task submitted successfully"
                    })
                else:
                    await task_mgr.update_task_status(task.task_id, TaskStatus.FAILED, "Failed to send to queue")
                    results.append({
                        "task_id": task.task_id,
                        "status": "failed",
                        "message": "Failed to submit task to queue"
                    })
                    
            except Exception as e:
                results.append({
                    "task_id": request.task_id,
                    "status": "error",
                    "message": str(e)
                })
        
        return {"results": results}
        
    except Exception as e:
        logger.error(f"Failed to submit batch tasks: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit batch tasks: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )