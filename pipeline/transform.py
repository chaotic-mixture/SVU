from typing import Dict, List, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from pathlib import Path
import yaml
from sqlalchemy.orm import Session
from models.database import Item, Price, ExchangeRate

logger = logging.getLogger(__name__)

class DataTransformationPipeline:
    """数据转换管道类"""
    
    def __init__(self,
                 db_session: Session,
                 config_path: str = 'config/api_config.yaml'):
        """初始化数据转换管道
        
        Args:
            db_session: 数据库会话
            config_path: 配置文件路径
        """
        self.db = db_session
        self.config = self._load_config(config_path)
        
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
            
    def normalize_prices(self,
                        prices: pd.DataFrame,
                        base_item_id: int,
                        window: str = '1D') -> pd.DataFrame:
        """标准化价格数据
        
        Args:
            prices: 价格数据
            base_item_id: 基准物品ID
            window: 时间窗口
            
        Returns:
            pd.DataFrame: 标准化后的价格数据
        """
        try:
            # 确保时间戳列是datetime类型
            prices['timestamp'] = pd.to_datetime(prices['timestamp'])
            
            # 获取基准物品的价格
            base_prices = prices[prices['item_id'] == base_item_id].set_index('timestamp')
            
            # 按时间窗口重采样基准价格
            base_prices = base_prices.resample(window).mean()
            
            # 标准化其他物品的价格
            normalized_prices = []
            for item_id in prices['item_id'].unique():
                if item_id == base_item_id:
                    continue
                    
                item_prices = prices[prices['item_id'] == item_id].set_index('timestamp')
                item_prices = item_prices.resample(window).mean()
                
                # 计算相对价格
                relative_prices = item_prices['price'] / base_prices['price']
                
                normalized_prices.append(pd.DataFrame({
                    'timestamp': relative_prices.index,
                    'item_id': item_id,
                    'normalized_price': relative_prices.values
                }))
                
            return pd.concat(normalized_prices, ignore_index=True)
            
        except Exception as e:
            logger.error(f"标准化价格失败: {str(e)}")
            return pd.DataFrame()
            
    def calculate_volatility(self,
                           prices: pd.DataFrame,
                           window: str = '30D') -> pd.DataFrame:
        """计算价格波动率
        
        Args:
            prices: 价格数据
            window: 时间窗口
            
        Returns:
            pd.DataFrame: 包含波动率的价格数据
        """
        try:
            # 确保时间戳列是datetime类型
            prices['timestamp'] = pd.to_datetime(prices['timestamp'])
            
            # 按物品ID分组计算波动率
            volatility_data = []
            for item_id in prices['item_id'].unique():
                item_prices = prices[prices['item_id'] == item_id].set_index('timestamp')
                
                # 计算滚动波动率
                returns = item_prices['price'].pct_change()
                volatility = returns.rolling(window=window).std() * np.sqrt(252)  # 年化波动率
                
                volatility_data.append(pd.DataFrame({
                    'timestamp': volatility.index,
                    'item_id': item_id,
                    'volatility': volatility.values
                }))
                
            return pd.concat(volatility_data, ignore_index=True)
            
        except Exception as e:
            logger.error(f"计算波动率失败: {str(e)}")
            return pd.DataFrame()
            
    def calculate_correlation(self,
                            prices: pd.DataFrame,
                            window: str = '90D') -> pd.DataFrame:
        """计算价格相关性
        
        Args:
            prices: 价格数据
            window: 时间窗口
            
        Returns:
            pd.DataFrame: 相关性矩阵
        """
        try:
            # 确保时间戳列是datetime类型
            prices['timestamp'] = pd.to_datetime(prices['timestamp'])
            
            # 将数据透视为宽格式
            wide_prices = prices.pivot(index='timestamp',
                                     columns='item_id',
                                     values='price')
            
            # 计算滚动相关性
            correlation = wide_prices.rolling(window=window).corr()
            
            return correlation
            
        except Exception as e:
            logger.error(f"计算相关性失败: {str(e)}")
            return pd.DataFrame()
            
    def detect_anomalies(self,
                        prices: pd.DataFrame,
                        threshold: float = 3.0) -> pd.DataFrame:
        """检测价格异常
        
        Args:
            prices: 价格数据
            threshold: 异常阈值（标准差的倍数）
            
        Returns:
            pd.DataFrame: 包含异常标记的数据
        """
        try:
            # 确保时间戳列是datetime类型
            prices['timestamp'] = pd.to_datetime(prices['timestamp'])
            
            # 按物品ID分组检测异常
            anomaly_data = []
            for item_id in prices['item_id'].unique():
                item_prices = prices[prices['item_id'] == item_id]
                
                # 计算价格变化
                price_changes = item_prices['price'].pct_change()
                
                # 计算均值和标准差
                mean_change = price_changes.mean()
                std_change = price_changes.std()
                
                # 标记异常
                anomalies = abs(price_changes - mean_change) > (threshold * std_change)
                
                anomaly_data.append(pd.DataFrame({
                    'timestamp': item_prices['timestamp'],
                    'item_id': item_id,
                    'price': item_prices['price'],
                    'is_anomaly': anomalies
                }))
                
            return pd.concat(anomaly_data, ignore_index=True)
            
        except Exception as e:
            logger.error(f"检测异常失败: {str(e)}")
            return pd.DataFrame()
            
    def calculate_market_metrics(self,
                               prices: pd.DataFrame,
                               window: str = '1D') -> pd.DataFrame:
        """计算市场指标
        
        Args:
            prices: 价格数据
            window: 时间窗口
            
        Returns:
            pd.DataFrame: 市场指标数据
        """
        try:
            # 确保时间戳列是datetime类型
            prices['timestamp'] = pd.to_datetime(prices['timestamp'])
            
            # 按时间窗口重采样
            resampled = prices.set_index('timestamp').resample(window)
            
            # 计算市场指标
            metrics = pd.DataFrame({
                'timestamp': resampled['price'].mean().index,
                'mean_price': resampled['price'].mean().values,
                'price_std': resampled['price'].std().values,
                'price_min': resampled['price'].min().values,
                'price_max': resampled['price'].max().values,
                'price_range': resampled['price'].max().values - resampled['price'].min().values,
                'price_median': resampled['price'].median().values
            })
            
            return metrics
            
        except Exception as e:
            logger.error(f"计算市场指标失败: {str(e)}")
            return pd.DataFrame()
            
    def calculate_trend_indicators(self,
                                 prices: pd.DataFrame,
                                 short_window: str = '20D',
                                 long_window: str = '50D') -> pd.DataFrame:
        """计算趋势指标
        
        Args:
            prices: 价格数据
            short_window: 短期窗口
            long_window: 长期窗口
            
        Returns:
            pd.DataFrame: 趋势指标数据
        """
        try:
            # 确保时间戳列是datetime类型
            prices['timestamp'] = pd.to_datetime(prices['timestamp'])
            
            # 按物品ID分组计算趋势指标
            trend_data = []
            for item_id in prices['item_id'].unique():
                item_prices = prices[prices['item_id'] == item_id].set_index('timestamp')
                
                # 计算移动平均
                short_ma = item_prices['price'].rolling(window=short_window).mean()
                long_ma = item_prices['price'].rolling(window=long_window).mean()
                
                # 计算趋势指标
                trend = pd.DataFrame({
                    'timestamp': item_prices.index,
                    'item_id': item_id,
                    'short_ma': short_ma,
                    'long_ma': long_ma,
                    'trend': np.where(short_ma > long_ma, 1, -1)  # 1表示上升趋势，-1表示下降趋势
                })
                
                trend_data.append(trend)
                
            return pd.concat(trend_data, ignore_index=True)
            
        except Exception as e:
            logger.error(f"计算趋势指标失败: {str(e)}")
            return pd.DataFrame() 