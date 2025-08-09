"""
向量数据库管理器
支持多种向量数据库的连接和操作
"""
import logging
import asyncio
from typing import Dict, List, Optional, Any, Union, Tuple
from abc import ABC, abstractmethod
from datetime import datetime
import numpy as np
import uuid

# Weaviate
import weaviate
from weaviate.exceptions import WeaviateException

# Pinecone
try:
    import pinecone
    PINECONE_AVAILABLE = True
except ImportError:
    PINECONE_AVAILABLE = False

# Qdrant
try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qdrant_models
    QDRANT_AVAILABLE = True
except ImportError:
    QDRANT_AVAILABLE = False

# Chroma
try:
    import chromadb
    from chromadb.config import Settings as ChromaSettings
    CHROMA_AVAILABLE = True
except ImportError:
    CHROMA_AVAILABLE = False

# Milvus
try:
    from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType
    MILVUS_AVAILABLE = True
except ImportError:
    MILVUS_AVAILABLE = False

from ..common.models import VectorizationTask, VectorizationResult

logger = logging.getLogger(__name__)


class VectorDBConfig:
    """向量数据库配置"""
    
    def __init__(
        self,
        db_type: str,
        connection_params: Dict[str, Any],
        collection_name: str = "vectorization_results",
        **kwargs
    ):
        self.db_type = db_type  # weaviate, pinecone, qdrant, chroma, milvus
        self.connection_params = connection_params
        self.collection_name = collection_name
        self.extra_params = kwargs


class BaseVectorDB(ABC):
    """向量数据库基类"""
    
    def __init__(self, config: VectorDBConfig):
        self.config = config
        self.client = None
        self.is_connected = False
    
    @abstractmethod
    async def connect(self) -> bool:
        """连接到数据库"""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """断开数据库连接"""
        pass
    
    @abstractmethod
    async def create_collection(
        self,
        collection_name: str,
        dimension: int,
        **kwargs
    ) -> bool:
        """创建集合/索引"""
        pass
    
    @abstractmethod
    async def insert_vectors(
        self,
        collection_name: str,
        vectors: np.ndarray,
        metadata: List[Dict[str, Any]],
        ids: Optional[List[str]] = None
    ) -> bool:
        """插入向量"""
        pass
    
    @abstractmethod
    async def search_vectors(
        self,
        collection_name: str,
        query_vector: np.ndarray,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """搜索向量"""
        pass
    
    @abstractmethod
    async def delete_vectors(
        self,
        collection_name: str,
        ids: List[str]
    ) -> bool:
        """删除向量"""
        pass
    
    @abstractmethod
    async def get_collection_info(self, collection_name: str) -> Dict[str, Any]:
        """获取集合信息"""
        pass


class WeaviateDB(BaseVectorDB):
    """Weaviate向量数据库"""
    
    async def connect(self) -> bool:
        """连接到Weaviate"""
        try:
            url = self.config.connection_params.get('url', 'http://localhost:8080')
            api_key = self.config.connection_params.get('api_key')
            timeout = self.config.connection_params.get('timeout', 30)
            
            auth_config = None
            if api_key:
                auth_config = weaviate.AuthApiKey(api_key=api_key)
            
            self.client = weaviate.Client(
                url=url,
                auth_client_secret=auth_config,
                timeout_config=weaviate.config.Timeout(
                    query=timeout,
                    insert=timeout
                )
            )
            
            # 测试连接
            if self.client.is_ready():
                self.is_connected = True
                logger.info(f"Connected to Weaviate at {url}")
                return True
            else:
                logger.error("Weaviate is not ready")
                return False
                
        except Exception as e:
            logger.error(f"Failed to connect to Weaviate: {e}")
            return False
    
    async def disconnect(self):
        """断开Weaviate连接"""
        if self.client:
            self.client = None
            self.is_connected = False
            logger.info("Disconnected from Weaviate")
    
    async def create_collection(
        self,
        collection_name: str,
        dimension: int,
        **kwargs
    ) -> bool:
        """创建Weaviate类"""
        try:
            if not self.is_connected:
                await self.connect()
            
            # 检查类是否已存在
            if self.client.schema.exists(collection_name):
                logger.info(f"Weaviate class {collection_name} already exists")
                return True
            
            # 创建类定义
            class_definition = {
                "class": collection_name,
                "description": f"Vectorization results for {collection_name}",
                "vectorizer": "none",  # 我们提供自己的向量
                "properties": [
                    {
                        "name": "task_id",
                        "dataType": ["string"],
                        "description": "Task ID"
                    },
                    {
                        "name": "task_type",
                        "dataType": ["string"],
                        "description": "Task type"
                    },
                    {
                        "name": "model_name",
                        "dataType": ["string"],
                        "description": "Model used for vectorization"
                    },
                    {
                        "name": "original_data",
                        "dataType": ["text"],
                        "description": "Original data that was vectorized"
                    },
                    {
                        "name": "metadata",
                        "dataType": ["object"],
                        "description": "Additional metadata"
                    },
                    {
                        "name": "created_at",
                        "dataType": ["date"],
                        "description": "Creation timestamp"
                    }
                ]
            }
            
            # 添加自定义属性
            for key, value in kwargs.items():
                if key.startswith('property_'):
                    prop_name = key[9:]  # 移除 'property_' 前缀
                    class_definition["properties"].append({
                        "name": prop_name,
                        "dataType": ["string"],
                        "description": f"Custom property: {prop_name}"
                    })
            
            self.client.schema.create_class(class_definition)
            logger.info(f"Created Weaviate class: {collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create Weaviate class {collection_name}: {e}")
            return False
    
    async def insert_vectors(
        self,
        collection_name: str,
        vectors: np.ndarray,
        metadata: List[Dict[str, Any]],
        ids: Optional[List[str]] = None
    ) -> bool:
        """插入向量到Weaviate"""
        try:
            if not self.is_connected:
                await self.connect()
            
            if len(vectors) != len(metadata):
                raise ValueError("Vectors and metadata must have the same length")
            
            if ids and len(ids) != len(vectors):
                raise ValueError("IDs and vectors must have the same length")
            
            # 批量插入
            with self.client.batch as batch:
                batch.batch_size = min(100, len(vectors))
                
                for i, (vector, meta) in enumerate(zip(vectors, metadata)):
                    # 准备数据对象
                    data_object = {
                        "task_id": meta.get("task_id", ""),
                        "task_type": meta.get("task_type", ""),
                        "model_name": meta.get("model_name", ""),
                        "original_data": str(meta.get("original_data", "")),
                        "metadata": meta.get("extra_metadata", {}),
                        "created_at": datetime.utcnow().isoformat()
                    }
                    
                    # 添加自定义字段
                    for key, value in meta.items():
                        if key not in data_object and not key.startswith('_'):
                            data_object[key] = str(value)
                    
                    # 生成ID
                    object_id = ids[i] if ids else str(uuid.uuid4())
                    
                    batch.add_data_object(
                        data_object=data_object,
                        class_name=collection_name,
                        uuid=object_id,
                        vector=vector.tolist() if isinstance(vector, np.ndarray) else vector
                    )
            
            logger.info(f"Inserted {len(vectors)} vectors into Weaviate collection {collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert vectors into Weaviate: {e}")
            return False
    
    async def search_vectors(
        self,
        collection_name: str,
        query_vector: np.ndarray,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """在Weaviate中搜索向量"""
        try:
            if not self.is_connected:
                await self.connect()
            
            # 构建查询
            near_vector = {"vector": query_vector.tolist()}
            
            query = self.client.query.get(collection_name, [
                "task_id", "task_type", "model_name", "original_data", "metadata"
            ]).with_near_vector(near_vector).with_limit(top_k)
            
            # 添加过滤器
            if filters:
                where_filter = self._build_where_filter(filters)
                if where_filter:
                    query = query.with_where(where_filter)
            
            result = query.do()
            
            # 处理结果
            results = []
            if 'data' in result and 'Get' in result['data']:
                objects = result['data']['Get'].get(collection_name, [])
                for obj in objects:
                    results.append({
                        "id": obj.get("_additional", {}).get("id"),
                        "score": obj.get("_additional", {}).get("distance", 0),
                        "metadata": obj
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to search vectors in Weaviate: {e}")
            return []
    
    def _build_where_filter(self, filters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """构建Weaviate where过滤器"""
        if not filters:
            return None
        
        conditions = []
        for key, value in filters.items():
            conditions.append({
                "path": [key],
                "operator": "Equal",
                "valueString": str(value)
            })
        
        if len(conditions) == 1:
            return conditions[0]
        else:
            return {
                "operator": "And",
                "operands": conditions
            }
    
    async def delete_vectors(
        self,
        collection_name: str,
        ids: List[str]
    ) -> bool:
        """删除Weaviate中的向量"""
        try:
            if not self.is_connected:
                await self.connect()
            
            for object_id in ids:
                self.client.data_object.delete(
                    uuid=object_id,
                    class_name=collection_name
                )
            
            logger.info(f"Deleted {len(ids)} vectors from Weaviate collection {collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete vectors from Weaviate: {e}")
            return False
    
    async def get_collection_info(self, collection_name: str) -> Dict[str, Any]:
        """获取Weaviate类信息"""
        try:
            if not self.is_connected:
                await self.connect()
            
            schema = self.client.schema.get(collection_name)
            
            # 获取对象数量
            result = self.client.query.aggregate(collection_name).with_meta_count().do()
            count = 0
            if 'data' in result and 'Aggregate' in result['data']:
                agg_data = result['data']['Aggregate'].get(collection_name, [])
                if agg_data:
                    count = agg_data[0].get('meta', {}).get('count', 0)
            
            return {
                "name": collection_name,
                "schema": schema,
                "count": count,
                "type": "weaviate"
            }
            
        except Exception as e:
            logger.error(f"Failed to get Weaviate collection info: {e}")
            return {}


class QdrantDB(BaseVectorDB):
    """Qdrant向量数据库"""
    
    async def connect(self) -> bool:
        """连接到Qdrant"""
        if not QDRANT_AVAILABLE:
            logger.error("Qdrant client not available. Install with: pip install qdrant-client")
            return False
        
        try:
            url = self.config.connection_params.get('url', 'localhost')
            port = self.config.connection_params.get('port', 6333)
            api_key = self.config.connection_params.get('api_key')
            
            self.client = QdrantClient(
                host=url,
                port=port,
                api_key=api_key
            )
            
            # 测试连接
            collections = self.client.get_collections()
            self.is_connected = True
            logger.info(f"Connected to Qdrant at {url}:{port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Qdrant: {e}")
            return False
    
    async def disconnect(self):
        """断开Qdrant连接"""
        if self.client:
            self.client.close()
            self.client = None
            self.is_connected = False
            logger.info("Disconnected from Qdrant")
    
    async def create_collection(
        self,
        collection_name: str,
        dimension: int,
        **kwargs
    ) -> bool:
        """创建Qdrant集合"""
        try:
            if not self.is_connected:
                await self.connect()
            
            # 检查集合是否已存在
            try:
                self.client.get_collection(collection_name)
                logger.info(f"Qdrant collection {collection_name} already exists")
                return True
            except:
                pass
            
            # 创建集合
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=qdrant_models.VectorParams(
                    size=dimension,
                    distance=qdrant_models.Distance.COSINE
                )
            )
            
            logger.info(f"Created Qdrant collection: {collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create Qdrant collection {collection_name}: {e}")
            return False
    
    async def insert_vectors(
        self,
        collection_name: str,
        vectors: np.ndarray,
        metadata: List[Dict[str, Any]],
        ids: Optional[List[str]] = None
    ) -> bool:
        """插入向量到Qdrant"""
        try:
            if not self.is_connected:
                await self.connect()
            
            if len(vectors) != len(metadata):
                raise ValueError("Vectors and metadata must have the same length")
            
            # 准备点数据
            points = []
            for i, (vector, meta) in enumerate(zip(vectors, metadata)):
                point_id = ids[i] if ids else str(uuid.uuid4())
                
                point = qdrant_models.PointStruct(
                    id=point_id,
                    vector=vector.tolist() if isinstance(vector, np.ndarray) else vector,
                    payload=meta
                )
                points.append(point)
            
            # 批量插入
            self.client.upsert(
                collection_name=collection_name,
                points=points
            )
            
            logger.info(f"Inserted {len(vectors)} vectors into Qdrant collection {collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert vectors into Qdrant: {e}")
            return False
    
    async def search_vectors(
        self,
        collection_name: str,
        query_vector: np.ndarray,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """在Qdrant中搜索向量"""
        try:
            if not self.is_connected:
                await self.connect()
            
            # 构建过滤器
            query_filter = None
            if filters:
                conditions = []
                for key, value in filters.items():
                    conditions.append(
                        qdrant_models.FieldCondition(
                            key=key,
                            match=qdrant_models.MatchValue(value=value)
                        )
                    )
                
                if conditions:
                    query_filter = qdrant_models.Filter(must=conditions)
            
            # 搜索
            search_result = self.client.search(
                collection_name=collection_name,
                query_vector=query_vector.tolist(),
                query_filter=query_filter,
                limit=top_k
            )
            
            # 处理结果
            results = []
            for hit in search_result:
                results.append({
                    "id": hit.id,
                    "score": hit.score,
                    "metadata": hit.payload
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to search vectors in Qdrant: {e}")
            return []
    
    async def delete_vectors(
        self,
        collection_name: str,
        ids: List[str]
    ) -> bool:
        """删除Qdrant中的向量"""
        try:
            if not self.is_connected:
                await self.connect()
            
            self.client.delete(
                collection_name=collection_name,
                points_selector=qdrant_models.PointIdsList(
                    points=ids
                )
            )
            
            logger.info(f"Deleted {len(ids)} vectors from Qdrant collection {collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete vectors from Qdrant: {e}")
            return False
    
    async def get_collection_info(self, collection_name: str) -> Dict[str, Any]:
        """获取Qdrant集合信息"""
        try:
            if not self.is_connected:
                await self.connect()
            
            collection_info = self.client.get_collection(collection_name)
            
            return {
                "name": collection_name,
                "vectors_count": collection_info.vectors_count,
                "points_count": collection_info.points_count,
                "config": collection_info.config.dict(),
                "type": "qdrant"
            }
            
        except Exception as e:
            logger.error(f"Failed to get Qdrant collection info: {e}")
            return {}


class VectorDBManager:
    """向量数据库管理器"""
    
    def __init__(self, db_configs: Dict[str, VectorDBConfig], default_db: str = "weaviate"):
        """
        初始化向量数据库管理器
        
        Args:
            db_configs: 数据库配置字典
            default_db: 默认数据库
        """
        self.db_configs = db_configs
        self.default_db = default_db
        self.databases: Dict[str, BaseVectorDB] = {}
        self.lock = asyncio.Lock()
    
    def _create_database(self, db_name: str, config: VectorDBConfig) -> BaseVectorDB:
        """根据配置创建数据库实例"""
        if config.db_type == "weaviate":
            return WeaviateDB(config)
        elif config.db_type == "qdrant":
            return QdrantDB(config)
        # 可以添加更多数据库类型
        else:
            raise ValueError(f"Unsupported database type: {config.db_type}")
    
    async def get_database(self, db_name: Optional[str] = None) -> BaseVectorDB:
        """获取数据库实例"""
        if db_name is None:
            db_name = self.default_db
        
        async with self.lock:
            # 如果数据库已连接，直接返回
            if db_name in self.databases and self.databases[db_name].is_connected:
                return self.databases[db_name]
            
            # 检查配置是否存在
            if db_name not in self.db_configs:
                raise ValueError(f"Database {db_name} not configured")
            
            # 创建并连接数据库
            config = self.db_configs[db_name]
            db = self._create_database(db_name, config)
            
            if not await db.connect():
                raise RuntimeError(f"Failed to connect to database {db_name}")
            
            self.databases[db_name] = db
            logger.info(f"Database {db_name} connected and cached")
            return db
    
    async def store_vectors(
        self,
        task: VectorizationTask,
        vectors: np.ndarray,
        original_data: Union[str, List[str]],
        db_name: Optional[str] = None
    ) -> bool:
        """
        存储向量到数据库
        
        Args:
            task: 向量化任务
            vectors: 向量数据
            original_data: 原始数据
            db_name: 数据库名称
            
        Returns:
            bool: 存储是否成功
        """
        try:
            db = await self.get_database(db_name)
            
            # 确保集合存在
            collection_name = self.db_configs[db_name or self.default_db].collection_name
            await db.create_collection(collection_name, vectors.shape[1])
            
            # 准备元数据
            if isinstance(original_data, str):
                original_data = [original_data]
            
            metadata = []
            for i, data in enumerate(original_data):
                meta = {
                    "task_id": task.task_id,
                    "task_type": task.task_type.value,
                    "model_name": task.model_name,
                    "original_data": data,
                    "extra_metadata": task.metadata,
                    "created_at": datetime.utcnow().isoformat(),
                    "worker_id": task.worker_id or "",
                    "batch_index": i
                }
                metadata.append(meta)
            
            # 生成ID
            ids = [f"{task.task_id}_{i}" for i in range(len(metadata))]
            
            # 插入向量
            success = await db.insert_vectors(collection_name, vectors, metadata, ids)
            
            if success:
                logger.info(f"Stored {len(vectors)} vectors for task {task.task_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to store vectors: {e}")
            return False
    
    async def search_similar_vectors(
        self,
        query_vector: np.ndarray,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        db_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """搜索相似向量"""
        try:
            db = await self.get_database(db_name)
            collection_name = self.db_configs[db_name or self.default_db].collection_name
            
            results = await db.search_vectors(collection_name, query_vector, top_k, filters)
            return results
            
        except Exception as e:
            logger.error(f"Failed to search vectors: {e}")
            return []
    
    async def get_database_info(self, db_name: Optional[str] = None) -> Dict[str, Any]:
        """获取数据库信息"""
        try:
            db = await self.get_database(db_name)
            collection_name = self.db_configs[db_name or self.default_db].collection_name
            
            info = await db.get_collection_info(collection_name)
            info["db_name"] = db_name or self.default_db
            info["is_connected"] = db.is_connected
            
            return info
            
        except Exception as e:
            logger.error(f"Failed to get database info: {e}")
            return {}
    
    async def list_databases(self) -> List[Dict[str, Any]]:
        """列出所有配置的数据库"""
        databases_info = []
        for db_name, config in self.db_configs.items():
            info = {
                "name": db_name,
                "type": config.db_type,
                "collection_name": config.collection_name,
                "is_connected": db_name in self.databases and self.databases[db_name].is_connected
            }
            databases_info.append(info)
        
        return databases_info
    
    async def close_all_connections(self):
        """关闭所有数据库连接"""
        async with self.lock:
            for db_name, db in self.databases.items():
                try:
                    await db.disconnect()
                    logger.info(f"Disconnected from database {db_name}")
                except Exception as e:
                    logger.error(f"Error disconnecting from database {db_name}: {e}")
            
            self.databases.clear()
            logger.info("All database connections closed")