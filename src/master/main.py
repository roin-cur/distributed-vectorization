"""
主节点主应用程序
启动向量化系统的主节点服务
"""
import asyncio
import logging
import signal
import sys
from typing import Optional
import uvicorn
from prometheus_client import start_http_server

from .api import app
from .config import Settings, validate_settings, print_settings
from ..common.kafka_producer import KafkaProducerManager
from .task_manager import TaskManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 全局变量
kafka_producer: Optional[KafkaProducerManager] = None
task_manager: Optional[TaskManager] = None
settings = Settings()


class MasterNodeService:
    """主节点服务类"""
    
    def __init__(self):
        self.settings = Settings()
        self.kafka_producer: Optional[KafkaProducerManager] = None
        self.task_manager: Optional[TaskManager] = None
        self.uvicorn_server: Optional[uvicorn.Server] = None
        self.metrics_server_started = False
        
        # 信号处理
        self._shutdown_event = asyncio.Event()
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """设置信号处理器"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            asyncio.create_task(self.shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def initialize(self) -> bool:
        """
        初始化服务
        
        Returns:
            bool: 初始化是否成功
        """
        try:
            logger.info("Initializing Master Node Service...")
            
            # 验证配置
            if not validate_settings():
                logger.error("Configuration validation failed")
                return False
            
            # 打印配置信息
            print_settings()
            
            # 配置日志级别
            logging.getLogger().setLevel(getattr(logging, self.settings.log_level.upper()))
            
            # 初始化Kafka生产者
            logger.info("Initializing Kafka producer...")
            self.kafka_producer = KafkaProducerManager(self.settings.kafka_bootstrap_servers)
            
            if not await self.kafka_producer.initialize():
                logger.error("Failed to initialize Kafka producer")
                return False
            
            # 初始化任务管理器
            logger.info("Initializing task manager...")
            self.task_manager = TaskManager(self.kafka_producer, self.settings)
            
            if not await self.task_manager.initialize():
                logger.error("Failed to initialize task manager")
                return False
            
            # 启动Prometheus指标服务器
            if self.settings.metrics_enabled:
                await self._start_metrics_server()
            
            logger.info("Master Node Service initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Master Node Service: {e}")
            return False
    
    async def _start_metrics_server(self):
        """启动Prometheus指标服务器"""
        try:
            start_http_server(self.settings.metrics_port)
            self.metrics_server_started = True
            logger.info(f"Prometheus metrics server started on port {self.settings.metrics_port}")
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")
    
    async def start_api_server(self):
        """启动API服务器"""
        try:
            logger.info(f"Starting API server on {self.settings.host}:{self.settings.port}")
            
            config = uvicorn.Config(
                app=app,
                host=self.settings.host,
                port=self.settings.port,
                workers=1,  # 使用单进程，因为我们有共享状态
                log_level=self.settings.log_level.lower(),
                access_log=True,
                reload=False
            )
            
            self.uvicorn_server = uvicorn.Server(config)
            await self.uvicorn_server.serve()
            
        except Exception as e:
            logger.error(f"Failed to start API server: {e}")
            raise
    
    async def run(self):
        """运行主节点服务"""
        try:
            # 初始化服务
            if not await self.initialize():
                logger.error("Service initialization failed")
                sys.exit(1)
            
            # 启动API服务器
            logger.info("Master Node Service is starting...")
            await self.start_api_server()
            
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Service error: {e}")
            sys.exit(1)
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """关闭服务"""
        if self._shutdown_event.is_set():
            return
        
        self._shutdown_event.set()
        logger.info("Shutting down Master Node Service...")
        
        try:
            # 停止API服务器
            if self.uvicorn_server:
                logger.info("Stopping API server...")
                self.uvicorn_server.should_exit = True
                
                # 等待服务器停止
                if hasattr(self.uvicorn_server, 'force_exit'):
                    self.uvicorn_server.force_exit = True
            
            # 关闭任务管理器
            if self.task_manager:
                logger.info("Closing task manager...")
                await self.task_manager.close()
            
            # 关闭Kafka生产者
            if self.kafka_producer:
                logger.info("Closing Kafka producer...")
                await self.kafka_producer.close()
            
            logger.info("Master Node Service shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    async def health_check(self) -> bool:
        """健康检查"""
        try:
            # 检查Kafka连接
            if not self.kafka_producer or not self.kafka_producer.is_connected():
                logger.warning("Kafka connection unhealthy")
                return False
            
            # 检查任务管理器
            if not self.task_manager:
                logger.warning("Task manager not available")
                return False
            
            # 检查Redis连接
            if not await self.task_manager.check_redis_connection():
                logger.warning("Redis connection unhealthy")
                return False
            
            # 检查Weaviate连接
            if not await self.task_manager.check_weaviate_connection():
                logger.warning("Weaviate connection unhealthy")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False


async def main():
    """主函数"""
    service = MasterNodeService()
    
    try:
        await service.run()
    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)


def cli_main():
    """命令行入口点"""
    try:
        # 检查Python版本
        if sys.version_info < (3, 8):
            print("Python 3.8 or higher is required")
            sys.exit(1)
        
        # 运行服务
        asyncio.run(main())
        
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli_main()