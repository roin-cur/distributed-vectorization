# 分布式文本向量化服务 (Distributed Text Vectorization Service)

一个可扩展的分布式文本向量化服务，支持大规模文本数据的向量化处理和存储。

## 🏗️ 架构概览

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   API Gateway   │    │  Master Node    │    │  Worker Nodes   │
│                 │    │                 │    │                 │
│  - REST API     │───▶│  - 任务分发      │───▶│  - 文本向量化    │
│  - 任务提交     │    │  - 状态监控      │    │  - 批量处理      │
│  - 状态查询     │    │  - 负载均衡      │    │  - 结果写入      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Kafka       │    │   Weaviate      │    │  HuggingFace    │
│                 │    │                 │    │                 │
│  - 任务队列      │    │  - 向量存储      │    │  - 预训练模型    │
│  - 消息重试      │    │  - 语义搜索      │    │  - 文本编码      │
│  - 负载均衡      │    │  - 数据持久化    │    │  - GPU加速      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 核心特性

- **分布式架构**: 主从节点分离，支持水平扩展
- **高可用性**: 基于Kafka的消息队列，支持任务重试和故障恢复
- **批量处理**: 工作节点批量消费任务，提高处理效率
- **向量存储**: 集成Weaviate向量数据库，支持高效的语义搜索
- **模型支持**: 支持HuggingFace生态的各种预训练模型
- **容器化部署**: 完整的Docker支持，便于部署和扩展
- **监控告警**: 内置健康检查和性能监控

## 📁 项目结构

```
distributed-vectorization/
├── src/                          # 源代码目录
│   ├── common/                   # 通用组件
│   │   ├── __init__.py
│   │   ├── config.py            # 配置管理
│   │   ├── logging.py           # 日志配置
│   │   ├── kafka_client.py      # Kafka客户端封装
│   │   ├── weaviate_client.py   # Weaviate客户端封装
│   │   └── models.py            # 数据模型定义
│   ├── master/                   # 主节点服务
│   │   ├── __init__.py
│   │   ├── api.py               # REST API服务
│   │   ├── task_manager.py      # 任务管理器
│   │   ├── scheduler.py         # 任务调度器
│   │   └── monitor.py           # 监控服务
│   ├── worker/                   # 工作节点服务
│   │   ├── __init__.py
│   │   ├── consumer.py          # Kafka消费者
│   │   ├── vectorizer.py        # 文本向量化处理器
│   │   ├── batch_processor.py   # 批量处理器
│   │   └── health_check.py      # 健康检查
│   └── utils/                    # 工具函数
│       ├── __init__.py
│       ├── text_processor.py    # 文本预处理
│       ├── model_loader.py      # 模型加载器
│       └── metrics.py           # 性能指标
├── docker/                       # Docker配置
│   ├── Dockerfile.master        # 主节点镜像
│   ├── Dockerfile.worker        # 工作节点镜像
│   └── docker-compose.yml       # 本地开发环境
├── config/                       # 配置文件
│   ├── master.yaml              # 主节点配置
│   ├── worker.yaml              # 工作节点配置
│   └── kafka.properties         # Kafka配置
├── scripts/                      # 部署和管理脚本
│   ├── setup.sh                 # 环境初始化
│   ├── start_master.py          # 启动主节点
│   ├── start_worker.py          # 启动工作节点
│   └── health_check.py          # 健康检查脚本
├── tests/                        # 测试代码
│   ├── unit/                    # 单元测试
│   ├── integration/             # 集成测试
│   └── performance/             # 性能测试
├── docs/                         # 文档目录
│   ├── architecture.md         # 架构设计文档
│   ├── api.md                   # API文档
│   ├── deployment.md            # 部署指南
│   └── development.md           # 开发指南
├── requirements.txt              # Python依赖
├── requirements-dev.txt          # 开发依赖
├── .env.example                 # 环境变量示例
├── .gitignore                   # Git忽略文件
└── README.md                    # 项目说明
```

## 🛠️ 技术栈

- **后端框架**: FastAPI (异步高性能)
- **消息队列**: Apache Kafka (高吞吐量消息处理)
- **向量数据库**: Weaviate (语义搜索和向量存储)
- **机器学习**: HuggingFace Transformers (预训练模型)
- **容器化**: Docker & Docker Compose
- **监控**: Prometheus + Grafana (可选)
- **日志**: Structured Logging with JSON format

## 🚀 快速开始

### 环境要求

- Python 3.9+
- Docker & Docker Compose
- 至少8GB内存 (推荐16GB+)
- GPU支持 (可选，用于加速向量化)

### 本地开发环境

1. **克隆项目**
```bash
git clone <repository-url>
cd distributed-vectorization
```

2. **环境配置**
```bash
# 复制环境变量配置
cp .env.example .env

# 安装Python依赖
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

3. **启动基础服务**
```bash
# 启动Kafka和Weaviate
docker-compose up -d kafka weaviate

# 等待服务就绪
./scripts/health_check.py
```

4. **启动服务**
```bash
# 启动主节点
python scripts/start_master.py

# 启动工作节点 (另一个终端)
python scripts/start_worker.py
```

### Docker部署

```bash
# 构建镜像
docker-compose build

# 启动完整服务栈
docker-compose up -d

# 查看服务状态
docker-compose ps
```

## 📊 API使用示例

### 提交向量化任务

```bash
curl -X POST "http://localhost:8000/api/v1/vectorize" \
  -H "Content-Type: application/json" \
  -d '{
    "texts": ["Hello world", "Machine learning is awesome"],
    "model_name": "sentence-transformers/all-MiniLM-L6-v2",
    "batch_size": 32
  }'
```

### 查询任务状态

```bash
curl "http://localhost:8000/api/v1/tasks/{task_id}/status"
```

### 语义搜索

```bash
curl -X POST "http://localhost:8000/api/v1/search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "artificial intelligence",
    "limit": 10,
    "threshold": 0.7
  }'
```

## 🔧 配置说明

主要配置文件位于 `config/` 目录下：

- `master.yaml`: 主节点配置 (API端口、Kafka连接等)
- `worker.yaml`: 工作节点配置 (批处理大小、模型设置等)
- `kafka.properties`: Kafka连接和主题配置

详细配置说明请参考 [配置文档](docs/deployment.md)。

## 📈 性能优化

- **批量处理**: 工作节点支持批量处理文本，提高GPU利用率
- **模型缓存**: 智能模型加载和缓存机制
- **连接池**: 数据库连接池优化
- **异步处理**: 全异步架构，提高并发性能

## 🔍 监控和告警

- 健康检查端点: `/health`
- 性能指标端点: `/metrics`
- 任务统计面板: `/dashboard`

## 🤝 贡献指南

1. Fork项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启Pull Request

## 📄 许可证

本项目采用MIT许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🆘 支持

如果你遇到问题或有建议，请：

1. 查看 [文档](docs/)
2. 搜索 [Issues](../../issues)
3. 创建新的 [Issue](../../issues/new)

---

**注意**: 这是一个开发中的项目，部分功能可能还在完善中。欢迎贡献代码和反馈！