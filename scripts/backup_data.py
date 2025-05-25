import os
import sys
import logging
import shutil
from datetime import datetime
from pathlib import Path
import yaml
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.database import Base, Item, Price, ExchangeRate

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/backup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

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

def backup_database():
    """备份数据库"""
    try:
        # 创建备份目录
        backup_dir = Path('data/backup')
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成备份文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = backup_dir / f'svu_data_{timestamp}.db'
        
        # 复制数据库文件
        shutil.copy2('data/database/svu_data.db', backup_file)
        
        logger.info(f"成功备份数据库到: {backup_file}")
        
        return backup_file
        
    except Exception as e:
        logger.error(f"备份数据库失败: {str(e)}")
        raise

def backup_config():
    """备份配置文件"""
    try:
        # 创建备份目录
        backup_dir = Path('data/backup')
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成备份文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = backup_dir / f'api_config_{timestamp}.yaml'
        
        # 复制配置文件
        shutil.copy2('config/api_config.yaml', backup_file)
        
        logger.info(f"成功备份配置文件到: {backup_file}")
        
        return backup_file
        
    except Exception as e:
        logger.error(f"备份配置文件失败: {str(e)}")
        raise

def backup_data_files():
    """备份数据文件"""
    try:
        # 创建备份目录
        backup_dir = Path('data/backup')
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成备份文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = backup_dir / f'data_files_{timestamp}'
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 备份原始数据
        raw_dir = Path('data/raw')
        if raw_dir.exists():
            shutil.copytree(raw_dir, backup_dir / 'raw')
            
        # 备份处理后的数据
        processed_dir = Path('data/processed')
        if processed_dir.exists():
            shutil.copytree(processed_dir, backup_dir / 'processed')
            
        logger.info(f"成功备份数据文件到: {backup_dir}")
        
        return backup_dir
        
    except Exception as e:
        logger.error(f"备份数据文件失败: {str(e)}")
        raise

def cleanup_old_backups(config):
    """清理旧备份"""
    try:
        # 获取最大备份文件数
        max_backup_files = config['data_processing']['storage']['max_backup_files']
        
        # 获取备份目录
        backup_dir = Path('data/backup')
        
        # 获取所有备份文件
        backup_files = []
        for file in backup_dir.glob('*'):
            if file.is_file():
                backup_files.append((file, file.stat().st_mtime))
                
        # 按修改时间排序
        backup_files.sort(key=lambda x: x[1], reverse=True)
        
        # 删除多余的备份文件
        for file, _ in backup_files[max_backup_files:]:
            file.unlink()
            logger.info(f"删除旧备份文件: {file}")
            
        logger.info("成功清理旧备份")
        
    except Exception as e:
        logger.error(f"清理旧备份失败: {str(e)}")
        raise

def export_data_to_csv():
    """导出数据到CSV文件"""
    try:
        # 创建导出目录
        export_dir = Path('data/backup/csv')
        export_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成导出文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_dir = export_dir / timestamp
        export_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建数据库连接
        engine = create_engine('sqlite:///data/database/svu_data.db')
        
        # 导出价格数据
        prices = pd.read_sql('SELECT * FROM prices', engine)
        prices.to_csv(export_dir / 'prices.csv', index=False)
        
        # 导出汇率数据
        rates = pd.read_sql('SELECT * FROM exchange_rates', engine)
        rates.to_csv(export_dir / 'exchange_rates.csv', index=False)
        
        # 导出物品数据
        items = pd.read_sql('SELECT * FROM items', engine)
        items.to_csv(export_dir / 'items.csv', index=False)
        
        logger.info(f"成功导出数据到CSV文件: {export_dir}")
        
        return export_dir
        
    except Exception as e:
        logger.error(f"导出数据到CSV文件失败: {str(e)}")
        raise

def main():
    """主函数"""
    try:
        # 加载配置
        config = load_config()
        
        # 备份数据库
        db_backup = backup_database()
        
        # 备份配置文件
        config_backup = backup_config()
        
        # 备份数据文件
        data_backup = backup_data_files()
        
        # 导出数据到CSV
        csv_export = export_data_to_csv()
        
        # 清理旧备份
        cleanup_old_backups(config)
        
        logger.info("备份完成")
        logger.info(f"数据库备份: {db_backup}")
        logger.info(f"配置文件备份: {config_backup}")
        logger.info(f"数据文件备份: {data_backup}")
        logger.info(f"CSV导出: {csv_export}")
        
    except Exception as e:
        logger.error(f"运行备份脚本失败: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 