"""
向量化模型管理器
支持多种向量化模型的加载、管理和推理
"""
import os
import logging
import asyncio
from typing import Dict, List, Optional, Union, Any
from pathlib import Path
import torch
import numpy as np
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModel
import clip
from PIL import Image
import cv2
from concurrent.futures import ThreadPoolExecutor

from ..common.models import TaskType

logger = logging.getLogger(__name__)


class ModelConfig:
    """模型配置类"""
    
    def __init__(
        self,
        name: str,
        model_type: str,
        model_path: str,
        max_length: int = 512,
        batch_size: int = 32,
        device: str = "auto",
        precision: str = "float32",
        cache_dir: Optional[str] = None
    ):
        self.name = name
        self.model_type = model_type  # sentence_transformer, huggingface, clip, custom
        self.model_path = model_path
        self.max_length = max_length
        self.batch_size = batch_size
        self.device = device
        self.precision = precision
        self.cache_dir = cache_dir


class BaseVectorizer:
    """基础向量化器抽象类"""
    
    def __init__(self, config: ModelConfig):
        self.config = config
        self.model = None
        self.tokenizer = None
        self.device = self._get_device()
        self.is_loaded = False
    
    def _get_device(self) -> str:
        """获取设备"""
        if self.config.device == "auto":
            if torch.cuda.is_available():
                return "cuda"
            elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
                return "mps"
            else:
                return "cpu"
        return self.config.device
    
    async def load(self):
        """加载模型"""
        raise NotImplementedError
    
    async def unload(self):
        """卸载模型"""
        if self.model is not None:
            del self.model
            self.model = None
        if self.tokenizer is not None:
            del self.tokenizer
            self.tokenizer = None
        
        # 清理GPU内存
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        
        self.is_loaded = False
    
    async def encode(self, data: Union[str, List[str], Any]) -> np.ndarray:
        """编码数据为向量"""
        raise NotImplementedError
    
    def get_embedding_dim(self) -> int:
        """获取向量维度"""
        raise NotImplementedError


class SentenceTransformerVectorizer(BaseVectorizer):
    """Sentence Transformer向量化器"""
    
    async def load(self):
        """加载Sentence Transformer模型"""
        try:
            logger.info(f"Loading Sentence Transformer model: {self.config.model_path}")
            
            # 在线程池中加载模型避免阻塞
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                self.model = await loop.run_in_executor(
                    executor,
                    lambda: SentenceTransformer(
                        self.config.model_path,
                        device=self.device,
                        cache_folder=self.config.cache_dir
                    )
                )
            
            self.is_loaded = True
            logger.info(f"Model loaded successfully on {self.device}")
            
        except Exception as e:
            logger.error(f"Failed to load Sentence Transformer model: {e}")
            raise
    
    async def encode(self, data: Union[str, List[str]]) -> np.ndarray:
        """编码文本为向量"""
        if not self.is_loaded:
            await self.load()
        
        try:
            # 在线程池中执行推理
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                embeddings = await loop.run_in_executor(
                    executor,
                    lambda: self.model.encode(
                        data,
                        batch_size=self.config.batch_size,
                        convert_to_numpy=True,
                        show_progress_bar=False
                    )
                )
            
            return embeddings
            
        except Exception as e:
            logger.error(f"Failed to encode data: {e}")
            raise
    
    def get_embedding_dim(self) -> int:
        """获取向量维度"""
        if not self.is_loaded:
            raise RuntimeError("Model not loaded")
        return self.model.get_sentence_embedding_dimension()


class HuggingFaceVectorizer(BaseVectorizer):
    """Hugging Face Transformers向量化器"""
    
    async def load(self):
        """加载Hugging Face模型"""
        try:
            logger.info(f"Loading Hugging Face model: {self.config.model_path}")
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                # 加载tokenizer和model
                self.tokenizer = await loop.run_in_executor(
                    executor,
                    lambda: AutoTokenizer.from_pretrained(
                        self.config.model_path,
                        cache_dir=self.config.cache_dir
                    )
                )
                
                self.model = await loop.run_in_executor(
                    executor,
                    lambda: AutoModel.from_pretrained(
                        self.config.model_path,
                        cache_dir=self.config.cache_dir
                    ).to(self.device)
                )
            
            self.model.eval()
            self.is_loaded = True
            logger.info(f"Hugging Face model loaded successfully on {self.device}")
            
        except Exception as e:
            logger.error(f"Failed to load Hugging Face model: {e}")
            raise
    
    async def encode(self, data: Union[str, List[str]]) -> np.ndarray:
        """编码文本为向量"""
        if not self.is_loaded:
            await self.load()
        
        try:
            if isinstance(data, str):
                data = [data]
            
            embeddings = []
            
            # 批处理
            for i in range(0, len(data), self.config.batch_size):
                batch = data[i:i + self.config.batch_size]
                
                # 在线程池中执行推理
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as executor:
                    batch_embeddings = await loop.run_in_executor(
                        executor,
                        self._encode_batch,
                        batch
                    )
                
                embeddings.append(batch_embeddings)
            
            return np.vstack(embeddings)
            
        except Exception as e:
            logger.error(f"Failed to encode data: {e}")
            raise
    
    def _encode_batch(self, batch: List[str]) -> np.ndarray:
        """编码一个批次的数据"""
        # Tokenize
        inputs = self.tokenizer(
            batch,
            padding=True,
            truncation=True,
            max_length=self.config.max_length,
            return_tensors="pt"
        ).to(self.device)
        
        # Forward pass
        with torch.no_grad():
            outputs = self.model(**inputs)
            # 使用CLS token或平均池化
            embeddings = outputs.last_hidden_state.mean(dim=1)
        
        return embeddings.cpu().numpy()
    
    def get_embedding_dim(self) -> int:
        """获取向量维度"""
        if not self.is_loaded:
            raise RuntimeError("Model not loaded")
        return self.model.config.hidden_size


class CLIPVectorizer(BaseVectorizer):
    """CLIP图像和文本向量化器"""
    
    async def load(self):
        """加载CLIP模型"""
        try:
            logger.info(f"Loading CLIP model: {self.config.model_path}")
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                self.model, self.preprocess = await loop.run_in_executor(
                    executor,
                    lambda: clip.load(self.config.model_path, device=self.device)
                )
            
            self.is_loaded = True
            logger.info(f"CLIP model loaded successfully on {self.device}")
            
        except Exception as e:
            logger.error(f"Failed to load CLIP model: {e}")
            raise
    
    async def encode(self, data: Union[str, List[str], Image.Image, List[Image.Image]]) -> np.ndarray:
        """编码文本或图像为向量"""
        if not self.is_loaded:
            await self.load()
        
        try:
            if isinstance(data, (str, Image.Image)):
                data = [data]
            
            # 判断数据类型
            if isinstance(data[0], str):
                return await self._encode_text(data)
            else:
                return await self._encode_images(data)
                
        except Exception as e:
            logger.error(f"Failed to encode data: {e}")
            raise
    
    async def _encode_text(self, texts: List[str]) -> np.ndarray:
        """编码文本"""
        embeddings = []
        
        for i in range(0, len(texts), self.config.batch_size):
            batch = texts[i:i + self.config.batch_size]
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                batch_embeddings = await loop.run_in_executor(
                    executor,
                    self._encode_text_batch,
                    batch
                )
            
            embeddings.append(batch_embeddings)
        
        return np.vstack(embeddings)
    
    async def _encode_images(self, images: List[Image.Image]) -> np.ndarray:
        """编码图像"""
        embeddings = []
        
        for i in range(0, len(images), self.config.batch_size):
            batch = images[i:i + self.config.batch_size]
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                batch_embeddings = await loop.run_in_executor(
                    executor,
                    self._encode_image_batch,
                    batch
                )
            
            embeddings.append(batch_embeddings)
        
        return np.vstack(embeddings)
    
    def _encode_text_batch(self, texts: List[str]) -> np.ndarray:
        """编码文本批次"""
        text_tokens = clip.tokenize(texts).to(self.device)
        
        with torch.no_grad():
            text_features = self.model.encode_text(text_tokens)
            text_features = text_features / text_features.norm(dim=-1, keepdim=True)
        
        return text_features.cpu().numpy()
    
    def _encode_image_batch(self, images: List[Image.Image]) -> np.ndarray:
        """编码图像批次"""
        image_tensors = torch.stack([
            self.preprocess(img) for img in images
        ]).to(self.device)
        
        with torch.no_grad():
            image_features = self.model.encode_image(image_tensors)
            image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        
        return image_features.cpu().numpy()
    
    def get_embedding_dim(self) -> int:
        """获取向量维度"""
        if not self.is_loaded:
            raise RuntimeError("Model not loaded")
        return self.model.visual.output_dim


class ModelManager:
    """模型管理器"""
    
    def __init__(self, models_config: Dict[str, ModelConfig], max_loaded_models: int = 3):
        """
        初始化模型管理器
        
        Args:
            models_config: 模型配置字典
            max_loaded_models: 最大同时加载的模型数量
        """
        self.models_config = models_config
        self.max_loaded_models = max_loaded_models
        self.loaded_models: Dict[str, BaseVectorizer] = {}
        self.model_usage_count: Dict[str, int] = {}
        self.lock = asyncio.Lock()
    
    def _create_vectorizer(self, config: ModelConfig) -> BaseVectorizer:
        """根据配置创建向量化器"""
        if config.model_type == "sentence_transformer":
            return SentenceTransformerVectorizer(config)
        elif config.model_type == "huggingface":
            return HuggingFaceVectorizer(config)
        elif config.model_type == "clip":
            return CLIPVectorizer(config)
        else:
            raise ValueError(f"Unsupported model type: {config.model_type}")
    
    async def get_model(self, model_name: str) -> BaseVectorizer:
        """获取模型实例"""
        async with self.lock:
            # 如果模型已加载，直接返回
            if model_name in self.loaded_models:
                self.model_usage_count[model_name] += 1
                return self.loaded_models[model_name]
            
            # 检查模型配置是否存在
            if model_name not in self.models_config:
                raise ValueError(f"Model {model_name} not configured")
            
            # 如果加载的模型数量已达上限，卸载最少使用的模型
            if len(self.loaded_models) >= self.max_loaded_models:
                await self._unload_least_used_model()
            
            # 创建并加载新模型
            config = self.models_config[model_name]
            vectorizer = self._create_vectorizer(config)
            
            try:
                await vectorizer.load()
                self.loaded_models[model_name] = vectorizer
                self.model_usage_count[model_name] = 1
                
                logger.info(f"Model {model_name} loaded and cached")
                return vectorizer
                
            except Exception as e:
                logger.error(f"Failed to load model {model_name}: {e}")
                raise
    
    async def _unload_least_used_model(self):
        """卸载使用次数最少的模型"""
        if not self.loaded_models:
            return
        
        # 找到使用次数最少的模型
        least_used_model = min(
            self.model_usage_count.items(),
            key=lambda x: x[1]
        )[0]
        
        # 卸载模型
        vectorizer = self.loaded_models.pop(least_used_model)
        await vectorizer.unload()
        del self.model_usage_count[least_used_model]
        
        logger.info(f"Unloaded model {least_used_model} to free memory")
    
    async def encode_data(
        self,
        model_name: str,
        data: Union[str, List[str], Any],
        task_type: TaskType
    ) -> np.ndarray:
        """
        使用指定模型编码数据
        
        Args:
            model_name: 模型名称
            data: 待编码数据
            task_type: 任务类型
            
        Returns:
            np.ndarray: 编码后的向量
        """
        try:
            vectorizer = await self.get_model(model_name)
            
            # 根据任务类型预处理数据
            processed_data = await self._preprocess_data(data, task_type)
            
            # 编码
            embeddings = await vectorizer.encode(processed_data)
            
            logger.debug(f"Encoded {len(processed_data) if isinstance(processed_data, list) else 1} items using {model_name}")
            return embeddings
            
        except Exception as e:
            logger.error(f"Failed to encode data with model {model_name}: {e}")
            raise
    
    async def _preprocess_data(self, data: Any, task_type: TaskType) -> Any:
        """根据任务类型预处理数据"""
        if task_type == TaskType.TEXT_VECTORIZATION:
            return data
        elif task_type == TaskType.BATCH_VECTORIZATION:
            return data
        elif task_type == TaskType.IMAGE_VECTORIZATION:
            # 处理图像数据
            if isinstance(data, str):
                # 如果是文件路径，加载图像
                return Image.open(data)
            return data
        elif task_type == TaskType.DOCUMENT_VECTORIZATION:
            # 处理文档数据，提取文本
            if isinstance(data, dict) and 'text' in data:
                return data['text']
            return data
        else:
            return data
    
    async def get_model_info(self, model_name: str) -> Dict[str, Any]:
        """获取模型信息"""
        if model_name not in self.models_config:
            raise ValueError(f"Model {model_name} not configured")
        
        config = self.models_config[model_name]
        is_loaded = model_name in self.loaded_models
        
        info = {
            "name": model_name,
            "type": config.model_type,
            "path": config.model_path,
            "device": config.device,
            "is_loaded": is_loaded,
            "usage_count": self.model_usage_count.get(model_name, 0)
        }
        
        if is_loaded:
            vectorizer = self.loaded_models[model_name]
            info["embedding_dim"] = vectorizer.get_embedding_dim()
            info["actual_device"] = vectorizer.device
        
        return info
    
    async def list_models(self) -> List[Dict[str, Any]]:
        """列出所有配置的模型"""
        models_info = []
        for model_name in self.models_config.keys():
            info = await self.get_model_info(model_name)
            models_info.append(info)
        return models_info
    
    async def unload_model(self, model_name: str):
        """卸载指定模型"""
        async with self.lock:
            if model_name in self.loaded_models:
                vectorizer = self.loaded_models.pop(model_name)
                await vectorizer.unload()
                del self.model_usage_count[model_name]
                logger.info(f"Model {model_name} unloaded")
    
    async def unload_all_models(self):
        """卸载所有模型"""
        async with self.lock:
            for model_name, vectorizer in self.loaded_models.items():
                await vectorizer.unload()
            
            self.loaded_models.clear()
            self.model_usage_count.clear()
            logger.info("All models unloaded")
    
    def get_loaded_models(self) -> List[str]:
        """获取已加载的模型列表"""
        return list(self.loaded_models.keys())
    
    def get_memory_usage(self) -> Dict[str, Any]:
        """获取内存使用情况"""
        memory_info = {
            "loaded_models_count": len(self.loaded_models),
            "max_loaded_models": self.max_loaded_models,
            "loaded_models": list(self.loaded_models.keys())
        }
        
        if torch.cuda.is_available():
            memory_info["gpu_memory"] = {
                "allocated": torch.cuda.memory_allocated(),
                "cached": torch.cuda.memory_reserved(),
                "max_allocated": torch.cuda.max_memory_allocated()
            }
        
        return memory_info