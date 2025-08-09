"""
任务处理器核心逻辑
负责处理向量化任务的核心业务逻辑
"""
import logging
import asyncio
import time
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
import numpy as np
from PIL import Image
import traceback

from ..common.models import VectorizationTask, VectorizationResult, TaskStatus, TaskType
from ..common.kafka_consumer import ResultProducer
from .model_manager import ModelManager
from .vector_db_manager import VectorDBManager

logger = logging.getLogger(__name__)


class TaskProcessor:
    """任务处理器"""
    
    def __init__(
        self,
        model_manager: ModelManager,
        vector_db_manager: VectorDBManager,
        result_producer: ResultProducer,
        worker_id: str
    ):
        """
        初始化任务处理器
        
        Args:
            model_manager: 模型管理器
            vector_db_manager: 向量数据库管理器
            result_producer: 结果生产者
            worker_id: 工作节点ID
        """
        self.model_manager = model_manager
        self.vector_db_manager = vector_db_manager
        self.result_producer = result_producer
        self.worker_id = worker_id
        
        # 处理统计
        self.stats = {
            'tasks_processed': 0,
            'tasks_succeeded': 0,
            'tasks_failed': 0,
            'total_vectors_generated': 0,
            'total_processing_time': 0.0,
            'last_task_time': None
        }
    
    async def process_task(self, task: VectorizationTask) -> VectorizationResult:
        """
        处理向量化任务
        
        Args:
            task: 向量化任务
            
        Returns:
            VectorizationResult: 处理结果
        """
        start_time = time.time()
        
        try:
            logger.info(f"Processing task {task.task_id} of type {task.task_type.value}")
            
            # 更新任务状态为处理中
            task.status = TaskStatus.PROCESSING
            task.worker_id = self.worker_id
            task.started_at = datetime.utcnow()
            
            await self.result_producer.send_status_update(
                task.task_id,
                TaskStatus.PROCESSING.value,
                {"worker_id": self.worker_id, "progress": 0.0}
            )
            
            # 根据任务类型处理
            if task.task_type == TaskType.TEXT_VECTORIZATION:
                result = await self._process_text_task(task)
            elif task.task_type == TaskType.BATCH_VECTORIZATION:
                result = await self._process_batch_task(task)
            elif task.task_type == TaskType.IMAGE_VECTORIZATION:
                result = await self._process_image_task(task)
            elif task.task_type == TaskType.DOCUMENT_VECTORIZATION:
                result = await self._process_document_task(task)
            else:
                raise ValueError(f"Unsupported task type: {task.task_type}")
            
            # 计算处理时间
            processing_time = time.time() - start_time
            result.processing_time = processing_time
            result.model_used = task.model_name
            result.completed_at = datetime.utcnow()
            
            # 更新统计信息
            self._update_stats(True, processing_time, len(result.vectors) if result.vectors else 0)
            
            # 发送完成状态
            await self.result_producer.send_status_update(
                task.task_id,
                TaskStatus.COMPLETED.value,
                {
                    "worker_id": self.worker_id,
                    "progress": 1.0,
                    "processing_time": processing_time,
                    "vectors_count": result.embeddings_count
                }
            )
            
            # 发送结果
            await self.result_producer.send_task_result(
                task.task_id,
                result.model_dump()
            )
            
            logger.info(f"Task {task.task_id} completed successfully in {processing_time:.2f}s")
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_message = str(e)
            
            logger.error(f"Task {task.task_id} failed: {error_message}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # 更新统计信息
            self._update_stats(False, processing_time, 0)
            
            # 创建失败结果
            result = VectorizationResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error_message=error_message,
                processing_time=processing_time,
                completed_at=datetime.utcnow()
            )
            
            # 发送失败状态
            await self.result_producer.send_status_update(
                task.task_id,
                TaskStatus.FAILED.value,
                {
                    "worker_id": self.worker_id,
                    "error_message": error_message,
                    "processing_time": processing_time
                }
            )
            
            # 发送失败结果
            await self.result_producer.send_task_result(
                task.task_id,
                result.model_dump()
            )
            
            return result
    
    async def _process_text_task(self, task: VectorizationTask) -> VectorizationResult:
        """处理文本向量化任务"""
        try:
            # 验证数据
            if not isinstance(task.data, str):
                raise ValueError("Text vectorization task requires string data")
            
            if not task.data.strip():
                raise ValueError("Text data cannot be empty")
            
            # 进度更新
            await self.result_producer.send_status_update(
                task.task_id,
                TaskStatus.PROCESSING.value,
                {"worker_id": self.worker_id, "progress": 0.2}
            )
            
            # 向量化
            vectors = await self.model_manager.encode_data(
                task.model_name,
                task.data,
                task.task_type
            )
            
            # 进度更新
            await self.result_producer.send_status_update(
                task.task_id,
                TaskStatus.PROCESSING.value,
                {"worker_id": self.worker_id, "progress": 0.6}
            )
            
            # 存储到向量数据库
            success = await self.vector_db_manager.store_vectors(
                task, vectors, task.data
            )
            
            if not success:
                logger.warning(f"Failed to store vectors for task {task.task_id}")
            
            # 进度更新
            await self.result_producer.send_status_update(
                task.task_id,
                TaskStatus.PROCESSING.value,
                {"worker_id": self.worker_id, "progress": 0.9}
            )
            
            # 创建结果
            result = VectorizationResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                vectors=vectors.tolist(),
                embeddings_count=len(vectors) if vectors.ndim > 1 else 1,
                metadata={
                    "text_length": len(task.data),
                    "stored_in_db": success,
                    **task.metadata
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing text task {task.task_id}: {e}")
            raise
    
    async def _process_batch_task(self, task: VectorizationTask) -> VectorizationResult:
        """处理批量向量化任务"""
        try:
            # 验证数据
            if not isinstance(task.data, list):
                raise ValueError("Batch vectorization task requires list data")
            
            if not task.data:
                raise ValueError("Batch data cannot be empty")
            
            batch_size = task.batch_size or 32
            total_items = len(task.data)
            
            logger.info(f"Processing batch task with {total_items} items, batch_size={batch_size}")
            
            all_vectors = []
            processed_count = 0
            
            # 分批处理
            for i in range(0, total_items, batch_size):
                batch_data = task.data[i:i + batch_size]
                
                # 进度更新
                progress = (processed_count / total_items) * 0.8  # 80%用于向量化
                await self.result_producer.send_status_update(
                    task.task_id,
                    TaskStatus.PROCESSING.value,
                    {"worker_id": self.worker_id, "progress": progress}
                )
                
                # 向量化批次
                batch_vectors = await self.model_manager.encode_data(
                    task.model_name,
                    batch_data,
                    TaskType.TEXT_VECTORIZATION  # 批量任务通常是文本
                )
                
                all_vectors.append(batch_vectors)
                processed_count += len(batch_data)
                
                logger.debug(f"Processed batch {i//batch_size + 1}, items: {len(batch_data)}")
            
            # 合并所有向量
            if all_vectors:
                vectors = np.vstack(all_vectors)
            else:
                vectors = np.array([])
            
            # 进度更新
            await self.result_producer.send_status_update(
                task.task_id,
                TaskStatus.PROCESSING.value,
                {"worker_id": self.worker_id, "progress": 0.9}
            )
            
            # 存储到向量数据库
            success = await self.vector_db_manager.store_vectors(
                task, vectors, task.data
            )
            
            if not success:
                logger.warning(f"Failed to store vectors for batch task {task.task_id}")
            
            # 创建结果
            result = VectorizationResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                vectors=vectors.tolist(),
                embeddings_count=len(vectors),
                metadata={
                    "batch_size": batch_size,
                    "total_items": total_items,
                    "batches_processed": len(all_vectors),
                    "stored_in_db": success,
                    **task.metadata
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing batch task {task.task_id}: {e}")
            raise
    
    async def _process_image_task(self, task: VectorizationTask) -> VectorizationResult:
        """处理图像向量化任务"""
        try:
            # 验证数据
            if isinstance(task.data, str):
                # 如果是文件路径，加载图像
                try:
                    image = Image.open(task.data)
                    image_data = image
                except Exception as e:
                    raise ValueError(f"Failed to load image from path: {e}")
            elif hasattr(task.data, 'mode'):
                # PIL Image对象
                image_data = task.data
            else:
                raise ValueError("Image vectorization task requires image path or PIL Image")
            
            # 进度更新
            await self.result_producer.send_status_update(
                task.task_id,
                TaskStatus.PROCESSING.value,
                {"worker_id": self.worker_id, "progress": 0.3}
            )
            
            # 向量化
            vectors = await self.model_manager.encode_data(
                task.model_name,
                image_data,
                task.task_type
            )
            
            # 进度更新
            await self.result_producer.send_status_update(
                task.task_id,
                TaskStatus.PROCESSING.value,
                {"worker_id": self.worker_id, "progress": 0.7}
            )
            
            # 存储到向量数据库
            image_info = f"Image: {image_data.size}, mode: {image_data.mode}"
            success = await self.vector_db_manager.store_vectors(
                task, vectors, image_info
            )
            
            if not success:
                logger.warning(f"Failed to store vectors for image task {task.task_id}")
            
            # 创建结果
            result = VectorizationResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                vectors=vectors.tolist(),
                embeddings_count=len(vectors) if vectors.ndim > 1 else 1,
                metadata={
                    "image_size": image_data.size,
                    "image_mode": image_data.mode,
                    "stored_in_db": success,
                    **task.metadata
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing image task {task.task_id}: {e}")
            raise
    
    async def _process_document_task(self, task: VectorizationTask) -> VectorizationResult:
        """处理文档向量化任务"""
        try:
            # 验证和预处理文档数据
            if isinstance(task.data, str):
                # 如果是纯文本
                text_content = task.data
            elif isinstance(task.data, dict):
                # 如果是结构化文档
                if 'text' in task.data:
                    text_content = task.data['text']
                elif 'content' in task.data:
                    text_content = task.data['content']
                else:
                    # 将所有文本字段合并
                    text_parts = []
                    for key, value in task.data.items():
                        if isinstance(value, str):
                            text_parts.append(f"{key}: {value}")
                    text_content = "\n".join(text_parts)
            else:
                raise ValueError("Document vectorization task requires string or dict data")
            
            if not text_content.strip():
                raise ValueError("Document content cannot be empty")
            
            # 进度更新
            await self.result_producer.send_status_update(
                task.task_id,
                TaskStatus.PROCESSING.value,
                {"worker_id": self.worker_id, "progress": 0.2}
            )
            
            # 文档分块（如果文档很长）
            max_chunk_size = 1000  # 可配置
            if len(text_content) > max_chunk_size:
                chunks = self._split_document(text_content, max_chunk_size)
                logger.info(f"Document split into {len(chunks)} chunks")
            else:
                chunks = [text_content]
            
            # 进度更新
            await self.result_producer.send_status_update(
                task.task_id,
                TaskStatus.PROCESSING.value,
                {"worker_id": self.worker_id, "progress": 0.4}
            )
            
            # 向量化所有块
            all_vectors = []
            for i, chunk in enumerate(chunks):
                chunk_vectors = await self.model_manager.encode_data(
                    task.model_name,
                    chunk,
                    TaskType.TEXT_VECTORIZATION
                )
                all_vectors.append(chunk_vectors)
                
                # 进度更新
                progress = 0.4 + (i + 1) / len(chunks) * 0.4
                await self.result_producer.send_status_update(
                    task.task_id,
                    TaskStatus.PROCESSING.value,
                    {"worker_id": self.worker_id, "progress": progress}
                )
            
            # 合并向量
            if len(all_vectors) == 1:
                vectors = all_vectors[0]
            else:
                vectors = np.vstack(all_vectors)
            
            # 进度更新
            await self.result_producer.send_status_update(
                task.task_id,
                TaskStatus.PROCESSING.value,
                {"worker_id": self.worker_id, "progress": 0.9}
            )
            
            # 存储到向量数据库
            success = await self.vector_db_manager.store_vectors(
                task, vectors, chunks
            )
            
            if not success:
                logger.warning(f"Failed to store vectors for document task {task.task_id}")
            
            # 创建结果
            result = VectorizationResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                vectors=vectors.tolist(),
                embeddings_count=len(vectors),
                metadata={
                    "document_length": len(text_content),
                    "chunks_count": len(chunks),
                    "stored_in_db": success,
                    **task.metadata
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing document task {task.task_id}: {e}")
            raise
    
    def _split_document(self, text: str, max_chunk_size: int) -> List[str]:
        """
        将文档分割成块
        
        Args:
            text: 文档文本
            max_chunk_size: 最大块大小
            
        Returns:
            List[str]: 文档块列表
        """
        # 简单的分块策略：按段落分割，然后按句子分割
        paragraphs = text.split('\n\n')
        chunks = []
        current_chunk = ""
        
        for paragraph in paragraphs:
            if len(current_chunk) + len(paragraph) <= max_chunk_size:
                if current_chunk:
                    current_chunk += "\n\n" + paragraph
                else:
                    current_chunk = paragraph
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                
                # 如果单个段落太长，按句子分割
                if len(paragraph) > max_chunk_size:
                    sentences = paragraph.split('. ')
                    temp_chunk = ""
                    
                    for sentence in sentences:
                        if len(temp_chunk) + len(sentence) <= max_chunk_size:
                            if temp_chunk:
                                temp_chunk += ". " + sentence
                            else:
                                temp_chunk = sentence
                        else:
                            if temp_chunk:
                                chunks.append(temp_chunk)
                            temp_chunk = sentence
                    
                    current_chunk = temp_chunk
                else:
                    current_chunk = paragraph
        
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks
    
    def _update_stats(self, success: bool, processing_time: float, vectors_count: int):
        """更新处理统计信息"""
        self.stats['tasks_processed'] += 1
        self.stats['total_processing_time'] += processing_time
        self.stats['last_task_time'] = datetime.utcnow()
        
        if success:
            self.stats['tasks_succeeded'] += 1
            self.stats['total_vectors_generated'] += vectors_count
        else:
            self.stats['tasks_failed'] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """获取处理统计信息"""
        stats = self.stats.copy()
        
        # 计算平均处理时间
        if stats['tasks_processed'] > 0:
            stats['average_processing_time'] = stats['total_processing_time'] / stats['tasks_processed']
        else:
            stats['average_processing_time'] = 0.0
        
        # 计算成功率
        if stats['tasks_processed'] > 0:
            stats['success_rate'] = stats['tasks_succeeded'] / stats['tasks_processed']
        else:
            stats['success_rate'] = 0.0
        
        stats['worker_id'] = self.worker_id
        
        return stats
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            'tasks_processed': 0,
            'tasks_succeeded': 0,
            'tasks_failed': 0,
            'total_vectors_generated': 0,
            'total_processing_time': 0.0,
            'last_task_time': None
        }
        logger.info("Task processor statistics reset")