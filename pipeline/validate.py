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

class DataValidationPipeline:
    """数据验证管道类"""
    
    def __init__(self,
                 db_session: Session,
                 config_path: str = 'config/api_config.yaml'):
        """初始化数据验证管道
        
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
            
    def validate_price_data(self,
                          prices: pd.DataFrame,
                          min_price: float = 0.0,
                          max_price: float = float('inf'),
                          max_gap: str = '7D') -> Dict:
        """验证价格数据
        
        Args:
            prices: 价格数据
            min_price: 最小价格
            max_price: 最大价格
            max_gap: 最大时间间隔
            
        Returns:
            Dict: 验证结果
        """
        try:
            # 确保时间戳列是datetime类型
            prices['timestamp'] = pd.to_datetime(prices['timestamp'])
            
            # 初始化验证结果
            validation_results = {
                'total_records': len(prices),
                'valid_records': 0,
                'invalid_records': 0,
                'missing_values': 0,
                'out_of_range': 0,
                'large_gaps': 0,
                'duplicates': 0,
                'validation_passed': False
            }
            
            # 检查缺失值
            missing_values = prices.isnull().sum().sum()
            validation_results['missing_values'] = missing_values
            
            # 检查价格范围
            out_of_range = ((prices['price'] < min_price) | 
                          (prices['price'] > max_price)).sum()
            validation_results['out_of_range'] = out_of_range
            
            # 检查重复记录
            duplicates = prices.duplicated().sum()
            validation_results['duplicates'] = duplicates
            
            # 检查时间间隔
            large_gaps = 0
            for item_id in prices['item_id'].unique():
                item_prices = prices[prices['item_id'] == item_id].sort_values('timestamp')
                gaps = item_prices['timestamp'].diff() > pd.Timedelta(max_gap)
                large_gaps += gaps.sum()
            validation_results['large_gaps'] = large_gaps
            
            # 计算有效记录数
            validation_results['valid_records'] = (
                validation_results['total_records'] -
                validation_results['missing_values'] -
                validation_results['out_of_range'] -
                validation_results['duplicates']
            )
            
            validation_results['invalid_records'] = (
                validation_results['total_records'] -
                validation_results['valid_records']
            )
            
            # 判断验证是否通过
            validation_results['validation_passed'] = (
                validation_results['valid_records'] > 0 and
                validation_results['invalid_records'] == 0
            )
            
            return validation_results
            
        except Exception as e:
            logger.error(f"验证价格数据失败: {str(e)}")
            return {}
            
    def validate_exchange_rate_data(self,
                                  rates: pd.DataFrame,
                                  min_rate: float = 0.0,
                                  max_rate: float = float('inf'),
                                  max_gap: str = '1D') -> Dict:
        """验证汇率数据
        
        Args:
            rates: 汇率数据
            min_rate: 最小汇率
            max_rate: 最大汇率
            max_gap: 最大时间间隔
            
        Returns:
            Dict: 验证结果
        """
        try:
            # 确保时间戳列是datetime类型
            rates['timestamp'] = pd.to_datetime(rates['timestamp'])
            
            # 初始化验证结果
            validation_results = {
                'total_records': len(rates),
                'valid_records': 0,
                'invalid_records': 0,
                'missing_values': 0,
                'out_of_range': 0,
                'large_gaps': 0,
                'duplicates': 0,
                'validation_passed': False
            }
            
            # 检查缺失值
            missing_values = rates.isnull().sum().sum()
            validation_results['missing_values'] = missing_values
            
            # 检查汇率范围
            out_of_range = ((rates['rate'] < min_rate) | 
                          (rates['rate'] > max_rate)).sum()
            validation_results['out_of_range'] = out_of_range
            
            # 检查重复记录
            duplicates = rates.duplicated().sum()
            validation_results['duplicates'] = duplicates
            
            # 检查时间间隔
            large_gaps = 0
            for source_id in rates['source_item_id'].unique():
                for target_id in rates['target_item_id'].unique():
                    pair_rates = rates[
                        (rates['source_item_id'] == source_id) &
                        (rates['target_item_id'] == target_id)
                    ].sort_values('timestamp')
                    
                    if not pair_rates.empty:
                        gaps = pair_rates['timestamp'].diff() > pd.Timedelta(max_gap)
                        large_gaps += gaps.sum()
                        
            validation_results['large_gaps'] = large_gaps
            
            # 计算有效记录数
            validation_results['valid_records'] = (
                validation_results['total_records'] -
                validation_results['missing_values'] -
                validation_results['out_of_range'] -
                validation_results['duplicates']
            )
            
            validation_results['invalid_records'] = (
                validation_results['total_records'] -
                validation_results['valid_records']
            )
            
            # 判断验证是否通过
            validation_results['validation_passed'] = (
                validation_results['valid_records'] > 0 and
                validation_results['invalid_records'] == 0
            )
            
            return validation_results
            
        except Exception as e:
            logger.error(f"验证汇率数据失败: {str(e)}")
            return {}
            
    def validate_data_consistency(self,
                                prices: pd.DataFrame,
                                rates: pd.DataFrame) -> Dict:
        """验证数据一致性
        
        Args:
            prices: 价格数据
            rates: 汇率数据
            
        Returns:
            Dict: 验证结果
        """
        try:
            # 初始化验证结果
            validation_results = {
                'total_items': len(prices['item_id'].unique()),
                'total_rate_pairs': len(rates.groupby(['source_item_id', 'target_item_id'])),
                'missing_items': 0,
                'inconsistent_rates': 0,
                'validation_passed': False
            }
            
            # 检查缺失物品
            price_items = set(prices['item_id'].unique())
            rate_source_items = set(rates['source_item_id'].unique())
            rate_target_items = set(rates['target_item_id'].unique())
            
            all_items = price_items.union(rate_source_items).union(rate_target_items)
            validation_results['missing_items'] = len(all_items) - len(price_items)
            
            # 检查汇率一致性
            inconsistent_rates = 0
            for source_id in rates['source_item_id'].unique():
                for target_id in rates['target_item_id'].unique():
                    # 获取直接汇率
                    direct_rates = rates[
                        (rates['source_item_id'] == source_id) &
                        (rates['target_item_id'] == target_id)
                    ]
                    
                    if not direct_rates.empty:
                        # 获取反向汇率
                        reverse_rates = rates[
                            (rates['source_item_id'] == target_id) &
                            (rates['target_item_id'] == source_id)
                        ]
                        
                        if not reverse_rates.empty:
                            # 检查汇率乘积是否接近1
                            for _, direct in direct_rates.iterrows():
                                for _, reverse in reverse_rates.iterrows():
                                    if abs(direct['rate'] * reverse['rate'] - 1.0) > 0.01:
                                        inconsistent_rates += 1
                                        
            validation_results['inconsistent_rates'] = inconsistent_rates
            
            # 判断验证是否通过
            validation_results['validation_passed'] = (
                validation_results['missing_items'] == 0 and
                validation_results['inconsistent_rates'] == 0
            )
            
            return validation_results
            
        except Exception as e:
            logger.error(f"验证数据一致性失败: {str(e)}")
            return {}
            
    def validate_data_completeness(self,
                                 prices: pd.DataFrame,
                                 rates: pd.DataFrame,
                                 start_date: datetime,
                                 end_date: datetime,
                                 frequency: str = '1D') -> Dict:
        """验证数据完整性
        
        Args:
            prices: 价格数据
            rates: 汇率数据
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            
        Returns:
            Dict: 验证结果
        """
        try:
            # 确保时间戳列是datetime类型
            prices['timestamp'] = pd.to_datetime(prices['timestamp'])
            rates['timestamp'] = pd.to_datetime(rates['timestamp'])
            
            # 生成完整的时间序列
            date_range = pd.date_range(start=start_date,
                                     end=end_date,
                                     freq=frequency)
            
            # 初始化验证结果
            validation_results = {
                'total_periods': len(date_range),
                'missing_price_periods': 0,
                'missing_rate_periods': 0,
                'validation_passed': False
            }
            
            # 检查价格数据完整性
            for item_id in prices['item_id'].unique():
                item_prices = prices[prices['item_id'] == item_id]
                item_dates = set(item_prices['timestamp'])
                missing_dates = set(date_range) - item_dates
                validation_results['missing_price_periods'] += len(missing_dates)
                
            # 检查汇率数据完整性
            for source_id in rates['source_item_id'].unique():
                for target_id in rates['target_item_id'].unique():
                    pair_rates = rates[
                        (rates['source_item_id'] == source_id) &
                        (rates['target_item_id'] == target_id)
                    ]
                    pair_dates = set(pair_rates['timestamp'])
                    missing_dates = set(date_range) - pair_dates
                    validation_results['missing_rate_periods'] += len(missing_dates)
                    
            # 判断验证是否通过
            validation_results['validation_passed'] = (
                validation_results['missing_price_periods'] == 0 and
                validation_results['missing_rate_periods'] == 0
            )
            
            return validation_results
            
        except Exception as e:
            logger.error(f"验证数据完整性失败: {str(e)}")
            return {} 