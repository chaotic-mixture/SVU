from typing import Dict, List, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from pathlib import Path
import yaml
from sqlalchemy.orm import Session
from models.database import Item, Price, ExchangeRate
from pipeline.ingest import DataIngestionPipeline
from pipeline.transform import DataTransformationPipeline
from pipeline.validate import DataValidationPipeline

logger = logging.getLogger(__name__)

class DataPipelineManager:
    """数据管道管理器类"""
    
    def __init__(self,
                 db_session: Session,
                 config_path: str = 'config/api_config.yaml'):
        """初始化数据管道管理器
        
        Args:
            db_session: 数据库会话
            config_path: 配置文件路径
        """
        self.db = db_session
        self.config = self._load_config(config_path)
        
        # 初始化各个管道
        self.ingestion_pipeline = DataIngestionPipeline(db_session, config_path)
        self.transformation_pipeline = DataTransformationPipeline(db_session, config_path)
        self.validation_pipeline = DataValidationPipeline(db_session, config_path)
        
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
            
    def process_price_data(self,
                          data: pd.DataFrame,
                          source: str,
                          base_item_id: int,
                          min_price: float = 0.0,
                          max_price: float = float('inf'),
                          max_gap: str = '7D') -> Dict:
        """处理价格数据
        
        Args:
            data: 价格数据
            source: 数据来源
            base_item_id: 基准物品ID
            min_price: 最小价格
            max_price: 最大价格
            max_gap: 最大时间间隔
            
        Returns:
            Dict: 处理结果
        """
        try:
            # 验证数据
            validation_results = self.validation_pipeline.validate_price_data(
                data,
                min_price=min_price,
                max_price=max_price,
                max_gap=max_gap
            )
            
            if not validation_results['validation_passed']:
                logger.error("价格数据验证失败")
                return {
                    'success': False,
                    'validation_results': validation_results
                }
                
            # 摄入数据
            self.ingestion_pipeline.ingest_data(
                data,
                source=source,
                data_type='price'
            )
            
            # 转换数据
            normalized_prices = self.transformation_pipeline.normalize_prices(
                data,
                base_item_id=base_item_id
            )
            
            volatility = self.transformation_pipeline.calculate_volatility(data)
            market_metrics = self.transformation_pipeline.calculate_market_metrics(data)
            trend_indicators = self.transformation_pipeline.calculate_trend_indicators(data)
            
            # 合并数据
            merged_data = self.ingestion_pipeline.merge_data()
            
            # 保存到数据库
            self.ingestion_pipeline.save_to_database(merged_data)
            
            return {
                'success': True,
                'validation_results': validation_results,
                'statistics': self.ingestion_pipeline.get_statistics(),
                'normalized_prices': normalized_prices,
                'volatility': volatility,
                'market_metrics': market_metrics,
                'trend_indicators': trend_indicators
            }
            
        except Exception as e:
            logger.error(f"处理价格数据失败: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
            
    def process_exchange_rate_data(self,
                                 data: pd.DataFrame,
                                 source: str,
                                 min_rate: float = 0.0,
                                 max_rate: float = float('inf'),
                                 max_gap: str = '1D') -> Dict:
        """处理汇率数据
        
        Args:
            data: 汇率数据
            source: 数据来源
            min_rate: 最小汇率
            max_rate: 最大汇率
            max_gap: 最大时间间隔
            
        Returns:
            Dict: 处理结果
        """
        try:
            # 验证数据
            validation_results = self.validation_pipeline.validate_exchange_rate_data(
                data,
                min_rate=min_rate,
                max_rate=max_rate,
                max_gap=max_gap
            )
            
            if not validation_results['validation_passed']:
                logger.error("汇率数据验证失败")
                return {
                    'success': False,
                    'validation_results': validation_results
                }
                
            # 摄入数据
            self.ingestion_pipeline.ingest_data(
                data,
                source=source,
                data_type='exchange_rate'
            )
            
            # 合并数据
            merged_data = self.ingestion_pipeline.merge_data()
            
            # 保存到数据库
            self.ingestion_pipeline.save_to_database(merged_data)
            
            return {
                'success': True,
                'validation_results': validation_results,
                'statistics': self.ingestion_pipeline.get_statistics()
            }
            
        except Exception as e:
            logger.error(f"处理汇率数据失败: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
            
    def validate_data_quality(self,
                            start_date: datetime,
                            end_date: datetime,
                            frequency: str = '1D') -> Dict:
        """验证数据质量
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            
        Returns:
            Dict: 验证结果
        """
        try:
            # 获取价格数据
            prices = pd.read_sql(
                self.db.query(Price).statement,
                self.db.bind
            )
            
            # 获取汇率数据
            rates = pd.read_sql(
                self.db.query(ExchangeRate).statement,
                self.db.bind
            )
            
            # 验证数据一致性
            consistency_results = self.validation_pipeline.validate_data_consistency(
                prices,
                rates
            )
            
            # 验证数据完整性
            completeness_results = self.validation_pipeline.validate_data_completeness(
                prices,
                rates,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency
            )
            
            return {
                'success': True,
                'consistency_results': consistency_results,
                'completeness_results': completeness_results
            }
            
        except Exception as e:
            logger.error(f"验证数据质量失败: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
            
    def get_data_statistics(self) -> Dict:
        """获取数据统计信息
        
        Returns:
            Dict: 统计信息
        """
        try:
            # 获取价格数据
            prices = pd.read_sql(
                self.db.query(Price).statement,
                self.db.bind
            )
            
            # 获取汇率数据
            rates = pd.read_sql(
                self.db.query(ExchangeRate).statement,
                self.db.bind
            )
            
            # 计算价格统计信息
            price_stats = {
                'total_records': len(prices),
                'unique_items': len(prices['item_id'].unique()),
                'date_range': {
                    'start': prices['timestamp'].min(),
                    'end': prices['timestamp'].max()
                },
                'price_stats': {
                    'mean': prices['price'].mean(),
                    'std': prices['price'].std(),
                    'min': prices['price'].min(),
                    'max': prices['price'].max()
                }
            }
            
            # 计算汇率统计信息
            rate_stats = {
                'total_records': len(rates),
                'unique_pairs': len(rates.groupby(['source_item_id', 'target_item_id'])),
                'date_range': {
                    'start': rates['timestamp'].min(),
                    'end': rates['timestamp'].max()
                },
                'rate_stats': {
                    'mean': rates['rate'].mean(),
                    'std': rates['rate'].std(),
                    'min': rates['rate'].min(),
                    'max': rates['rate'].max()
                }
            }
            
            return {
                'success': True,
                'price_statistics': price_stats,
                'rate_statistics': rate_stats
            }
            
        except Exception as e:
            logger.error(f"获取数据统计信息失败: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
            
    def clear_data(self) -> None:
        """清空数据"""
        try:
            self.ingestion_pipeline.clear_data()
            logger.info("成功清空数据")
        except Exception as e:
            logger.error(f"清空数据失败: {str(e)}")
            raise 