# 主节点设置和使用指南

## 概述

主节点是分布式向量化系统的核心组件，负责：
- 接收客户端的向量化请求
- 将任务写入Kafka队列
- 管理任务状态和监控
- 提供REST API接口

## 功能特性

### 核心功能
- **任务接收**: 接收文本、图像、文档等向量化请求
- **队列管理**: 将任务发送到Kafka进行分布式处理
- **状态跟踪**: 实时跟踪任务执行状态
- **结果管理**: 存储和检索向量化结果
- **负载均衡**: 智能分配任务到可用工作节点

### API接口
- `POST /tasks/submit` - 提交向量化任务
- `GET /tasks/{task_id}` - 获取任务详情
- `GET /tasks` - 获取任务列表（支持过滤和分页）
- `DELETE /tasks/{task_id}` - 取消任务
- `POST /tasks/batch` - 批量提交任务
- `GET /health` - 健康检查
- `GET /metrics` - 系统指标

## 快速开始

### 1. 环境准备

确保已安装依赖：
```bash
pip install -r requirements.txt
```

### 2. 启动基础服务

使用Docker Compose启动Kafka、Redis、Weaviate等服务：
```bash
docker-compose up -d kafka redis weaviate
```

### 3. 配置环境变量

复制并编辑配置文件：
```bash
cp .env.example .env
# 编辑 .env 文件设置合适的配置
```

### 4. 启动主节点

```bash
# 方式1: 直接运行
python scripts/start_master.py

# 方式2: 使用模块方式
python -m src.master.main

# 方式3: 使用Docker
docker-compose up master
```

### 5. 验证服务

```bash
# 健康检查
curl http://localhost:8000/health

# 提交测试任务
curl -X POST http://localhost:8000/tasks/submit \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "text_vectorization",
    "data": "这是一个测试文本",
    "model_name": "sentence-transformers/all-MiniLM-L6-v2"
  }'
```

## 配置说明

### 环境变量配置

主要环境变量：

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `VECTORIZE_MASTER_HOST` | 0.0.0.0 | API服务监听地址 |
| `VECTORIZE_MASTER_PORT` | 8000 | API服务端口 |
| `VECTORIZE_MASTER_KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka服务器地址 |
| `VECTORIZE_MASTER_REDIS_URL` | redis://localhost:6379/0 | Redis连接URL |
| `VECTORIZE_MASTER_WEAVIATE_URL` | http://localhost:8080 | Weaviate连接URL |
| `VECTORIZE_MASTER_LOG_LEVEL` | INFO | 日志级别 |

### YAML配置文件

可以使用 `config/master.yaml` 进行详细配置，包括：
- 服务配置
- Kafka配置
- 数据库配置
- 任务配置
- 监控配置

## API使用示例

### 提交文本向量化任务

```python
import requests

# 提交单个文本
response = requests.post('http://localhost:8000/tasks/submit', json={
    "task_type": "text_vectorization",
    "data": "这是需要向量化的文本",
    "model_name": "sentence-transformers/all-MiniLM-L6-v2",
    "priority": 1
})

task_info = response.json()
print(f"任务ID: {task_info['task_id']}")
```

### 提交批量文本任务

```python
# 提交批量文本
response = requests.post('http://localhost:8000/tasks/submit', json={
    "task_type": "batch_vectorization", 
    "data": [
        "第一个文本",
        "第二个文本",
        "第三个文本"
    ],
    "batch_size": 2
})
```

### 查询任务状态

```python
task_id = "your-task-id"
response = requests.get(f'http://localhost:8000/tasks/{task_id}')
task = response.json()

print(f"状态: {task['status']}")
print(f"进度: {task['progress']}")
```

### 获取任务列表

```python
# 获取所有任务
response = requests.get('http://localhost:8000/tasks')

# 按状态过滤
response = requests.get('http://localhost:8000/tasks?status_filter=pending')

# 分页查询
response = requests.get('http://localhost:8000/tasks?page=1&page_size=10')
```

## 监控和指标

### 健康检查

```bash
curl http://localhost:8000/health
```

返回示例：
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T12:00:00Z",
  "kafka_connected": true,
  "weaviate_connected": true,
  "redis_connected": true,
  "active_workers": 2,
  "queue_size": 5
}
```

### 系统指标

```bash
curl http://localhost:8000/metrics
```

返回示例：
```json
{
  "total_tasks": 100,
  "pending_tasks": 5,
  "processing_tasks": 3,
  "completed_tasks": 90,
  "failed_tasks": 2,
  "average_processing_time": 15.5,
  "total_vectors_generated": 1500
}
```

### Prometheus指标

主节点会在端口9090暴露Prometheus指标：
```bash
curl http://localhost:9090/metrics
```

## 测试

### 使用测试客户端

```bash
python scripts/test_client.py
```

测试客户端会执行以下测试：
1. 健康检查
2. 单个任务提交
3. 批量任务提交
4. 任务状态查询
5. 错误处理
6. 系统监控

### 手动测试

```bash
# 1. 检查服务状态
curl http://localhost:8000/health

# 2. 提交测试任务
curl -X POST http://localhost:8000/tasks/submit \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "text_vectorization",
    "data": "测试文本内容",
    "priority": 1
  }'

# 3. 查看任务列表
curl http://localhost:8000/tasks

# 4. 查看系统指标
curl http://localhost:8000/metrics
```

## 故障排除

### 常见问题

1. **Kafka连接失败**
   - 检查Kafka服务是否启动
   - 验证连接地址和端口
   - 查看Kafka日志

2. **Redis连接失败**
   - 检查Redis服务状态
   - 验证连接URL格式
   - 检查网络连接

3. **Weaviate连接失败**
   - 确认Weaviate服务正常运行
   - 检查URL配置
   - 验证网络可达性

4. **任务提交失败**
   - 检查请求格式
   - 验证数据内容
   - 查看服务日志

### 日志查看

```bash
# Docker方式查看日志
docker-compose logs master

# 直接运行查看日志
# 日志会输出到控制台，级别可通过LOG_LEVEL环境变量控制
```

### 性能优化

1. **调整批处理大小**
   - 根据数据大小调整 `batch_size`
   - 平衡内存使用和处理效率

2. **优化Kafka配置**
   - 调整 `batch.size` 和 `linger.ms`
   - 根据网络情况调整超时设置

3. **Redis连接池**
   - 调整 `max_connections` 参数
   - 监控连接使用情况

4. **任务优先级**
   - 合理设置任务优先级
   - 重要任务使用高优先级

## 扩展和定制

### 添加新的任务类型

1. 在 `models.py` 中添加新的 `TaskType`
2. 在API中添加相应的处理逻辑
3. 更新工作节点处理代码

### 自定义监控指标

1. 在 `TaskManager` 中添加指标收集
2. 扩展 `TaskMetrics` 模型
3. 在API中暴露新指标

### 集成外部系统

1. 添加回调URL支持
2. 实现结果通知机制
3. 集成第三方监控系统