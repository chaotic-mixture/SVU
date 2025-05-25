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
        logging.FileHandler('logs/restore.log'),
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

def list_backups():
    """列出可用的备份"""
    try:
        # 获取备份目录
        backup_dir = Path('data/backup')
        
        # 获取数据库备份
        db_backups = []
        for file in backup_dir.glob('svu_data_*.db'):
            db_backups.append({
                'type': 'database',
                'file': file,
                'timestamp': datetime.fromtimestamp(file.stat().st_mtime)
            })
            
        # 获取配置文件备份
        config_backups = []
        for file in backup_dir.glob('api_config_*.yaml'):
            config_backups.append({
                'type': 'config',
                'file': file,
                'timestamp': datetime.fromtimestamp(file.stat().st_mtime)
            })
            
        # 获取数据文件备份
        data_backups = []
        for dir in backup_dir.glob('data_files_*'):
            if dir.is_dir():
                data_backups.append({
                    'type': 'data',
                    'dir': dir,
                    'timestamp': datetime.fromtimestamp(dir.stat().st_mtime)
                })
                
        # 获取CSV导出备份
        csv_backups = []
        for dir in backup_dir.glob('csv/*'):
            if dir.is_dir():
                csv_backups.append({
                    'type': 'csv',
                    'dir': dir,
                    'timestamp': datetime.fromtimestamp(dir.stat().st_mtime)
                })
                
        return {
            'database': sorted(db_backups, key=lambda x: x['timestamp'], reverse=True),
            'config': sorted(config_backups, key=lambda x: x['timestamp'], reverse=True),
            'data': sorted(data_backups, key=lambda x: x['timestamp'], reverse=True),
            'csv': sorted(csv_backups, key=lambda x: x['timestamp'], reverse=True)
        }
        
    except Exception as e:
        logger.error(f"列出备份失败: {str(e)}")
        raise

def restore_database(backup_file):
    """恢复数据库"""
    try:
        # 创建数据库目录
        db_dir = Path('data/database')
        db_dir.mkdir(parents=True, exist_ok=True)
        
        # 复制备份文件
        shutil.copy2(backup_file, 'data/database/svu_data.db')
        
        logger.info(f"成功从{backup_file}恢复数据库")
        
    except Exception as e:
        logger.error(f"恢复数据库失败: {str(e)}")
        raise

def restore_config(backup_file):
    """恢复配置文件"""
    try:
        # 创建配置目录
        config_dir = Path('config')
        config_dir.mkdir(parents=True, exist_ok=True)
        
        # 复制备份文件
        shutil.copy2(backup_file, 'config/api_config.yaml')
        
        logger.info(f"成功从{backup_file}恢复配置文件")
        
    except Exception as e:
        logger.error(f"恢复配置文件失败: {str(e)}")
        raise

def restore_data_files(backup_dir):
    """恢复数据文件"""
    try:
        # 恢复原始数据
        raw_dir = Path('data/raw')
        if (backup_dir / 'raw').exists():
            if raw_dir.exists():
                shutil.rmtree(raw_dir)
            shutil.copytree(backup_dir / 'raw', raw_dir)
            
        # 恢复处理后的数据
        processed_dir = Path('data/processed')
        if (backup_dir / 'processed').exists():
            if processed_dir.exists():
                shutil.rmtree(processed_dir)
            shutil.copytree(backup_dir / 'processed', processed_dir)
            
        logger.info(f"成功从{backup_dir}恢复数据文件")
        
    except Exception as e:
        logger.error(f"恢复数据文件失败: {str(e)}")
        raise

def restore_from_csv(backup_dir):
    """从CSV文件恢复数据"""
    try:
        # 创建数据库连接
        engine = create_engine('sqlite:///data/database/svu_data.db')
        
        # 恢复价格数据
        if (backup_dir / 'prices.csv').exists():
            prices = pd.read_csv(backup_dir / 'prices.csv')
            prices.to_sql('prices', engine, if_exists='replace', index=False)
            
        # 恢复汇率数据
        if (backup_dir / 'exchange_rates.csv').exists():
            rates = pd.read_csv(backup_dir / 'exchange_rates.csv')
            rates.to_sql('exchange_rates', engine, if_exists='replace', index=False)
            
        # 恢复物品数据
        if (backup_dir / 'items.csv').exists():
            items = pd.read_csv(backup_dir / 'items.csv')
            items.to_sql('items', engine, if_exists='replace', index=False)
            
        logger.info(f"成功从{backup_dir}恢复CSV数据")
        
    except Exception as e:
        logger.error(f"从CSV文件恢复数据失败: {str(e)}")
        raise

def main():
    """主函数"""
    try:
        # 加载配置
        config = load_config()
        
        # 列出可用的备份
        backups = list_backups()
        
        # 显示可用的备份
        logger.info("可用的备份:")
        for backup_type, backup_list in backups.items():
            logger.info(f"\n{backup_type}备份:")
            for backup in backup_list:
                logger.info(f"- {backup['timestamp']}: {backup['file' if 'file' in backup else 'dir']}")
                
        # 选择要恢复的备份
        # 这里可以根据需要实现交互式选择或使用命令行参数
        # 为了示例，我们使用最新的备份
        if backups['database']:
            restore_database(backups['database'][0]['file'])
            
        if backups['config']:
            restore_config(backups['config'][0]['file'])
            
        if backups['data']:
            restore_data_files(backups['data'][0]['dir'])
            
        if backups['csv']:
            restore_from_csv(backups['csv'][0]['dir'])
            
        logger.info("恢复完成")
        
    except Exception as e:
        logger.error(f"运行恢复脚本失败: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 