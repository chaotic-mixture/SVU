from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union
import pandas as pd
from datetime import datetime, timedelta
import requests
import logging
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)

class BaseAPI(ABC):
    """API基础类，定义通用的API接口方法"""
    
    def __init__(self, config_path: str = 'config/api_config.yaml'):
        """初始化API
        
        Args:
            config_path: API配置文件路径
        """
        self.config = self._load_config(config_path)
        self.session = requests.Session()
        self.session.headers.update(self._get_headers())
        
    def _load_config(self, config_path: str) -> dict:
        """加载配置文件
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            dict: 配置信息
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            return {}
            
    def _get_headers(self) -> Dict[str, str]:
        """获取请求头
        
        Returns:
            Dict[str, str]: 请求头字典
        """
        return {
            'User-Agent': 'SVU-Data-Collector/1.0',
            'Accept': 'application/json'
        }
        
    @abstractmethod
    def get_historical_data(self,
                          start_date: str,
                          end_date: str,
                          frequency: str = 'monthly') -> pd.DataFrame:
        """获取历史数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            frequency: 数据频率 (daily, weekly, monthly, quarterly, yearly)
            
        Returns:
            pd.DataFrame: 历史数据
        """
        pass
        
    @abstractmethod
    def get_latest_data(self) -> pd.DataFrame:
        """获取最新数据
        
        Returns:
            pd.DataFrame: 最新数据
        """
        pass
        
    def save_data(self,
                 data: pd.DataFrame,
                 filename: str,
                 directory: str = 'data/raw') -> None:
        """保存数据
        
        Args:
            data: 要保存的数据
            filename: 文件名
            directory: 保存目录
        """
        try:
            # 创建目录
            save_dir = Path(directory)
            save_dir.mkdir(parents=True, exist_ok=True)
            
            # 保存数据
            filepath = save_dir / filename
            data.to_parquet(filepath)
            logger.info(f"数据已保存到: {filepath}")
            
        except Exception as e:
            logger.error(f"保存数据失败: {str(e)}")
            
    def resample_data(self,
                     data: pd.DataFrame,
                     frequency: str = 'monthly') -> pd.DataFrame:
        """重采样数据到指定频率
        
        Args:
            data: 原始数据
            frequency: 目标频率
            
        Returns:
            pd.DataFrame: 重采样后的数据
        """
        try:
            # 确保日期列是datetime类型
            if 'date' in data.columns:
                data['date'] = pd.to_datetime(data['date'])
                data.set_index('date', inplace=True)
            
            # 重采样
            resampled = data.resample(frequency[0]).agg({
                'price': 'mean',
                'volume': 'sum' if 'volume' in data.columns else None
            }).dropna()
            
            # 重置索引
            resampled.reset_index(inplace=True)
            
            return resampled
            
        except Exception as e:
            logger.error(f"重采样数据失败: {str(e)}")
            return data
            
    def validate_data(self,
                     data: pd.DataFrame,
                     required_columns: List[str]) -> bool:
        """验证数据
        
        Args:
            data: 要验证的数据
            required_columns: 必需的列名列表
            
        Returns:
            bool: 验证是否通过
        """
        try:
            # 检查必需的列
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                logger.error(f"缺少必需的列: {missing_columns}")
                return False
                
            # 检查数据类型
            if 'date' in data.columns:
                data['date'] = pd.to_datetime(data['date'])
                
            if 'price' in data.columns:
                data['price'] = pd.to_numeric(data['price'], errors='coerce')
                
            # 检查空值
            if data.isnull().any().any():
                logger.warning("数据中存在空值")
                
            return True
            
        except Exception as e:
            logger.error(f"验证数据失败: {str(e)}")
            return False
            
    def clean_data(self,
                  data: pd.DataFrame,
                  min_price: float = 0,
                  max_price: float = float('inf')) -> pd.DataFrame:
        """清洗数据
        
        Args:
            data: 要清洗的数据
            min_price: 最小价格
            max_price: 最大价格
            
        Returns:
            pd.DataFrame: 清洗后的数据
        """
        try:
            # 删除重复行
            data = data.drop_duplicates()
            
            # 删除价格异常值
            if 'price' in data.columns:
                data = data[
                    (data['price'] >= min_price) &
                    (data['price'] <= max_price)
                ]
                
            # 按日期排序
            if 'date' in data.columns:
                data = data.sort_values('date')
                
            return data
            
        except Exception as e:
            logger.error(f"清洗数据失败: {str(e)}")
            return data 