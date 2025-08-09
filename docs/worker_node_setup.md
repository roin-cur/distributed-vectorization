# 工作节点设置和使用指南

## 概述

工作节点是分布式向量化系统的执行单元，负责：
- 从Kafka队列消费向量化任务
- 使用本地模型执行向量化处理
- 将结果存储到向量数据库
- 发送处理状态和结果

## 功能特性

### 核心功能
- **任务消费**: 从Kafka队列消费向量化任务
- **多模型支持**: 支持Sentence Transformers、Hugging Face、CLIP等多种模型
- **灵活配置**: 可配置模型类型、设备、批处理大小等
- **多数据库支持**: 支持Weaviate、Qdrant等多种向量数据库
- **并发处理**: 支持多任务并发处理
- **状态监控**: 实时监控处理状态和性能指标

### 支持的模型类型

#### Sentence Transformers
- `sentence-transformers/all-MiniLM-L6-v2` (默认)
- `sentence-transformers/all-mpnet-base-v2`
- `sentence-transformers/distilbert-base-nli-stsb-mean-tokens`
- `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`

#### Hugging Face Transformers
- `bert-base-uncased`
- `distilbert-base-uncased`
- `roberta-base`

#### CLIP模型
- `ViT-B/32`
- `ViT-B/16`
- `ViT-L/14`

### 支持的向量数据库

#### Weaviate (默认)
- 完整的向量存储和检索功能
- 支持元数据过滤
- 自动创建集合/类

#### Qdrant
- 高性能向量搜索
- 支持过滤和聚合
- 分布式部署支持

#### 扩展支持
- Chroma
- Milvus
- Pinecone (计划中)

## 快速开始

### 1. 环境准备

确保已安装依赖：
```bash
pip install -r requirements.txt
```

### 2. 启动基础服务

使用Docker Compose启动依赖服务：
```bash
docker-compose up -d kafka redis weaviate
```

### 3. 配置环境变量

设置工作节点配置：
```bash
export VECTORIZE_WORKER_WORKER_ID=worker-1
export VECTORIZE_WORKER_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export VECTORIZE_WORKER_WEAVIATE_URL=http://localhost:8080
export VECTORIZE_WORKER_REDIS_URL=redis://localhost:6379/0
```

### 4. 启动工作节点

```bash
# 方式1: 直接运行
python scripts/start_worker.py

# 方式2: 使用模块方式
python -m src.worker.main

# 方式3: 使用Docker
docker-compose up worker-1
```

### 5. 验证功能

```bash
# 运行测试脚本
python scripts/test_worker.py

# 查看指标
curl http://localhost:9091/metrics
```

## 配置详解

### 环境变量配置

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `VECTORIZE_WORKER_WORKER_ID` | worker-1 | 工作节点唯一标识 |
| `VECTORIZE_WORKER_KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka服务器地址 |
| `VECTORIZE_WORKER_WEAVIATE_URL` | http://localhost:8080 | Weaviate连接URL |
| `VECTORIZE_WORKER_REDIS_URL` | redis://localhost:6379/0 | Redis连接URL |
| `VECTORIZE_WORKER_DEVICE` | auto | 计算设备 (auto/cpu/cuda/mps) |
| `VECTORIZE_WORKER_MAX_CONCURRENT_TASKS` | 5 | 最大并发任务数 |
| `VECTORIZE_WORKER_MAX_LOADED_MODELS` | 3 | 最大同时加载模型数 |
| `VECTORIZE_WORKER_DEFAULT_MODEL` | sentence-transformers/all-MiniLM-L6-v2 | 默认模型 |

### YAML配置文件

可以使用 `config/worker.yaml` 进行详细配置：

```yaml
# 基础配置
worker:
  id: "worker-1"
  name: "Vectorization Worker"

# 模型配置
models:
  cache_dir: "./models"
  max_loaded: 3
  default: "sentence-transformers/all-MiniLM-L6-v2"

# 设备配置
device:
  type: "auto"  # auto, cpu, cuda, mps
  gpu_memory_limit: null

# 处理配置
processing:
  max_concurrent_tasks: 5
  task_timeout: 1800
```

## 任务处理流程

### 1. 任务消费
- 从Kafka主题 `vectorization-tasks` 消费任务
- 解析任务数据和元数据
- 验证任务格式和参数

### 2. 模型加载
- 根据任务要求加载相应模型
- 智能缓存管理，自动卸载不常用模型
- 支持GPU/CPU自动切换

### 3. 数据处理
- 根据任务类型预处理数据
- 执行向量化编码
- 支持批处理优化

### 4. 结果存储
- 将向量存储到配置的向量数据库
- 保存元数据和索引信息
- 支持多数据库并行存储

### 5. 状态报告
- 发送处理状态更新到Kafka
- 发送最终结果到结果主题
- 更新心跳和监控指标

## 支持的任务类型

### 文本向量化
```json
{
  "task_type": "text_vectorization",
  "data": "这是需要向量化的文本",
  "model_name": "sentence-transformers/all-MiniLM-L6-v2"
}
```

### 批量向量化
```json
{
  "task_type": "batch_vectorization",
  "data": ["文本1", "文本2", "文本3"],
  "batch_size": 32
}
```

### 图像向量化
```json
{
  "task_type": "image_vectorization",
  "data": "/path/to/image.jpg",
  "model_name": "ViT-B/32"
}
```

### 文档向量化
```json
{
  "task_type": "document_vectorization",
  "data": {
    "title": "文档标题", 
    "content": "文档内容..."
  }
}
```

## 监控和指标

### Prometheus指标

工作节点在端口9091暴露Prometheus指标：

```bash
curl http://localhost:9091/metrics
```

主要指标：
- `worker_tasks_processed_total` - 处理的任务总数
- `worker_task_processing_seconds` - 任务处理时间分布
- `worker_loaded_models` - 已加载的模型数量
- `worker_memory_usage_bytes` - 内存使用量
- `worker_active_tasks` - 当前活跃任务数

### 心跳监控

工作节点定期向Redis发送心跳：
```bash
redis-cli get worker:worker-1:heartbeat
```

### 日志监控

工作节点产生详细的结构化日志：
```
2024-01-01 12:00:00 - worker.main - INFO - Processing task task-123 of type text_vectorization
2024-01-01 12:00:01 - worker.model_manager - INFO - Model sentence-transformers/all-MiniLM-L6-v2 loaded successfully
2024-01-01 12:00:02 - worker.task_processor - INFO - Task task-123 completed successfully in 1.50s
```

## 性能优化

### 模型管理优化

1. **模型缓存**
   - 调整 `max_loaded_models` 参数
   - 根据内存容量设置合适的缓存大小

2. **设备选择**
   - GPU加速：设置 `device=cuda`
   - Apple Silicon：设置 `device=mps`
   - CPU优化：设置 `device=cpu`

3. **批处理优化**
   - 调整 `default_batch_size` 和 `max_batch_size`
   - 根据GPU内存调整批处理大小

### 并发处理优化

1. **任务并发**
   - 调整 `max_concurrent_tasks`
   - 根据CPU核心数和内存设置

2. **Kafka消费优化**
   - 调整消费者组配置
   - 优化轮询间隔和批处理

### 向量数据库优化

1. **连接池管理**
   - 配置合适的连接超时
   - 使用连接池复用连接

2. **批量插入**
   - 启用批量插入模式
   - 调整批处理大小

## 故障排除

### 常见问题

1. **模型加载失败**
   ```
   错误: Failed to load model
   解决: 检查网络连接，确保有足够磁盘空间
   ```

2. **GPU内存不足**
   ```
   错误: CUDA out of memory
   解决: 减少batch_size或max_loaded_models
   ```

3. **Kafka连接失败**
   ```
   错误: Failed to connect to Kafka
   解决: 检查Kafka服务状态和网络连接
   ```

4. **向量数据库连接失败**
   ```
   错误: Failed to connect to Weaviate
   解决: 检查数据库服务状态和连接配置
   ```

### 调试技巧

1. **启用调试日志**
   ```bash
   export VECTORIZE_WORKER_LOG_LEVEL=DEBUG
   ```

2. **检查系统资源**
   ```bash
   # 检查GPU使用情况
   nvidia-smi
   
   # 检查内存使用
   free -h
   
   # 检查磁盘空间
   df -h
   ```

3. **测试模型加载**
   ```bash
   python scripts/test_worker.py
   ```

## 扩展和定制

### 添加新模型

1. 在配置中添加模型：
   ```yaml
   models:
     sentence_transformers:
       - "your-custom-model"
   ```

2. 实现自定义向量化器（如需要）

### 添加新向量数据库

1. 继承 `BaseVectorDB` 类
2. 实现必要的方法
3. 在配置中注册新数据库

### 自定义任务处理

1. 扩展 `TaskProcessor` 类
2. 添加新的任务类型处理逻辑
3. 更新任务路由

## 部署建议

### 单节点部署
- 适合开发和小规模使用
- 配置1-2个工作节点
- 使用本地存储

### 多节点部署
- 适合生产环境
- 配置多个工作节点实例
- 使用共享存储和负载均衡

### GPU集群部署
- 为每个GPU配置一个工作节点
- 使用CUDA_VISIBLE_DEVICES控制GPU分配
- 监控GPU使用率和温度

### 容器化部署
```bash
# 启动单个工作节点
docker-compose up worker-1

# 启动多个工作节点
docker-compose up worker-1 worker-2

# 扩展工作节点
docker-compose up --scale worker=4
```

## 最佳实践

1. **资源规划**
   - 根据任务量规划工作节点数量
   - 预留足够的内存和存储空间
   - 监控资源使用情况

2. **模型管理**
   - 选择合适的模型大小
   - 定期清理模型缓存
   - 使用模型版本管理

3. **监控告警**
   - 设置关键指标告警
   - 监控任务处理延迟
   - 跟踪错误率和成功率

4. **备份恢复**
   - 定期备份模型缓存
   - 备份配置文件
   - 制定故障恢复计划