import os
import sys
import logging
from pathlib import Path
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.database import Base, Item, Price, ExchangeRate
from pipeline.manager import DataPipelineManager

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def create_directories():
    """创建必要的目录"""
    try:
        # 创建数据目录
        os.makedirs('data/raw', exist_ok=True)
        os.makedirs('data/processed', exist_ok=True)
        os.makedirs('data/backup', exist_ok=True)
        
        # 创建日志目录
        os.makedirs('logs', exist_ok=True)
        
        logger.info("成功创建目录结构")
        
    except Exception as e:
        logger.error(f"创建目录失败: {str(e)}")
        raise

def init_database():
    """初始化数据库"""
    try:
        # 创建数据库引擎
        engine = create_engine('sqlite:///data/database/svu_data.db')
        
        # 创建所有表
        Base.metadata.create_all(engine)
        
        # 创建会话工厂
        Session = sessionmaker(bind=engine)
        session = Session()
        
        logger.info("成功初始化数据库")
        
        return session
        
    except Exception as e:
        logger.error(f"初始化数据库失败: {str(e)}")
        raise

def load_config():
    """加载配置文件"""
    try:
        config_path = 'config/api_config.yaml'
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        logger.info("成功加载配置文件")
        
        return config
        
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        raise

def init_pipeline():
    """初始化数据管道"""
    try:
        # 创建目录
        create_directories()
        
        # 初始化数据库
        session = init_database()
        
        # 加载配置
        config = load_config()
        
        # 创建数据管道管理器
        pipeline_manager = DataPipelineManager(session)
        
        logger.info("成功初始化数据管道")
        
        return pipeline_manager
        
    except Exception as e:
        logger.error(f"初始化数据管道失败: {str(e)}")
        raise

def main():
    """主函数"""
    try:
        # 初始化数据管道
        pipeline_manager = init_pipeline()
        
        # 获取数据统计信息
        stats = pipeline_manager.get_data_statistics()
        
        if stats['success']:
            logger.info("数据统计信息:")
            logger.info(f"价格数据: {stats['price_statistics']}")
            logger.info(f"汇率数据: {stats['rate_statistics']}")
        else:
            logger.error(f"获取数据统计信息失败: {stats['error']}")
            
    except Exception as e:
        logger.error(f"运行初始化脚本失败: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 