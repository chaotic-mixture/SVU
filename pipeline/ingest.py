from typing import Dict, List, Optional, Union
import pandas as pd
from datetime import datetime, timedelta
import logging
from pathlib import Path
import yaml
from dataclasses import dataclass
from sqlalchemy.orm import Session
from models.database import Item, Price, ExchangeRate

logger = logging.getLogger(__name__)

@dataclass
class DataPoint:
    """数据点类，表示统一的价格记录"""
    
    timestamp: datetime
    item_id: int
    value: float
    source: str
    confidence: float
    type: str  # 'price' 或 'exchange_rate'
    source_item_id: Optional[int] = None  # 仅用于汇率数据
    target_item_id: Optional[int] = None  # 仅用于汇率数据
    
    def to_dict(self) -> Dict:
        """转换为字典
        
        Returns:
            Dict: 数据点字典
        """
        data = {
            'timestamp': self.timestamp,
            'item_id': self.item_id,
            'value': self.value,
            'source': self.source,
            'confidence': self.confidence,
            'type': self.type
        }
        
        if self.type == 'exchange_rate':
            data.update({
                'source_item_id': self.source_item_id,
                'target_item_id': self.target_item_id
            })
            
        return data

class DataIngestionPipeline:
    """数据摄入管道类"""
    
    def __init__(self,
                 db_session: Session,
                 config_path: str = 'config/api_config.yaml'):
        """初始化数据摄入管道
        
        Args:
            db_session: 数据库会话
            config_path: 配置文件路径
        """
        self.db = db_session
        self.config = self._load_config(config_path)
        self.data_points: List[DataPoint] = []
        
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
            
    def ingest_data(self,
                   data: pd.DataFrame,
                   source: str,
                   data_type: str,
                   confidence: float = 1.0) -> None:
        """摄入数据
        
        Args:
            data: 数据DataFrame
            source: 数据来源
            data_type: 数据类型 ('price' 或 'exchange_rate')
            confidence: 数据置信度
        """
        try:
            # 验证数据
            required_columns = ['timestamp', 'value']
            if data_type == 'price':
                required_columns.append('item_id')
            elif data_type == 'exchange_rate':
                required_columns.extend(['source_item_id', 'target_item_id'])
                
            if not all(col in data.columns for col in required_columns):
                raise ValueError(f"数据缺少必需的列: {required_columns}")
                
            # 转换为DataPoint对象
            for _, row in data.iterrows():
                if data_type == 'price':
                    data_point = DataPoint(
                        timestamp=row['timestamp'],
                        item_id=row['item_id'],
                        value=row['value'],
                        source=source,
                        confidence=confidence,
                        type='price'
                    )
                else:
                    data_point = DataPoint(
                        timestamp=row['timestamp'],
                        item_id=row['source_item_id'],  # 使用源物品ID
                        value=row['value'],
                        source=source,
                        confidence=confidence,
                        type='exchange_rate',
                        source_item_id=row['source_item_id'],
                        target_item_id=row['target_item_id']
                    )
                    
                self.data_points.append(data_point)
                
            logger.info(f"成功摄入{len(data)}条{data_type}数据")
            
        except Exception as e:
            logger.error(f"摄入数据失败: {str(e)}")
            raise
            
    def merge_data(self,
                  priority_sources: List[str] = None,
                  min_confidence: float = 0.7) -> pd.DataFrame:
        """合并数据
        
        Args:
            priority_sources: 优先数据源列表
            min_confidence: 最小置信度
            
        Returns:
            pd.DataFrame: 合并后的数据
        """
        try:
            if not self.data_points:
                raise ValueError("没有数据点可供合并")
                
            # 转换为DataFrame
            df = pd.DataFrame([dp.to_dict() for dp in self.data_points])
            
            # 按时间戳和物品ID分组
            grouped = df.groupby(['timestamp', 'item_id'])
            
            # 合并数据
            merged_data = []
            for (timestamp, item_id), group in grouped:
                # 过滤低置信度数据
                group = group[group['confidence'] >= min_confidence]
                
                if group.empty:
                    continue
                    
                # 如果有优先数据源，优先使用它们的数据
                if priority_sources:
                    priority_group = group[group['source'].isin(priority_sources)]
                    if not priority_group.empty:
                        group = priority_group
                        
                # 选择置信度最高的数据
                best_data = group.loc[group['confidence'].idxmax()]
                merged_data.append(best_data)
                
            return pd.DataFrame(merged_data)
            
        except Exception as e:
            logger.error(f"合并数据失败: {str(e)}")
            return pd.DataFrame()
            
    def save_to_database(self, data: pd.DataFrame) -> None:
        """保存数据到数据库
        
        Args:
            data: 要保存的数据
        """
        try:
            # 分离价格和汇率数据
            price_data = data[data['type'] == 'price']
            rate_data = data[data['type'] == 'exchange_rate']
            
            # 保存价格数据
            for _, row in price_data.iterrows():
                price = Price(
                    item_id=row['item_id'],
                    price=row['value'],
                    timestamp=row['timestamp'],
                    source=row['source'],
                    confidence=row['confidence']
                )
                self.db.add(price)
                
            # 保存汇率数据
            for _, row in rate_data.iterrows():
                rate = ExchangeRate(
                    source_item_id=row['source_item_id'],
                    target_item_id=row['target_item_id'],
                    rate=row['value'],
                    timestamp=row['timestamp'],
                    source=row['source'],
                    confidence=row['confidence']
                )
                self.db.add(rate)
                
            # 提交事务
            self.db.commit()
            
            logger.info(f"成功保存{len(price_data)}条价格数据和{len(rate_data)}条汇率数据")
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"保存数据失败: {str(e)}")
            raise
            
    def clear_data(self) -> None:
        """清空数据点"""
        self.data_points.clear()
        
    def get_statistics(self) -> Dict:
        """获取数据统计信息
        
        Returns:
            Dict: 统计信息
        """
        try:
            if not self.data_points:
                return {
                    'total_points': 0,
                    'price_points': 0,
                    'rate_points': 0,
                    'sources': {},
                    'confidence_stats': {
                        'mean': 0.0,
                        'min': 0.0,
                        'max': 0.0
                    }
                }
                
            # 转换为DataFrame
            df = pd.DataFrame([dp.to_dict() for dp in self.data_points])
            
            # 计算统计信息
            stats = {
                'total_points': len(df),
                'price_points': len(df[df['type'] == 'price']),
                'rate_points': len(df[df['type'] == 'exchange_rate']),
                'sources': df['source'].value_counts().to_dict(),
                'confidence_stats': {
                    'mean': df['confidence'].mean(),
                    'min': df['confidence'].min(),
                    'max': df['confidence'].max()
                }
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"获取统计信息失败: {str(e)}")
            return {} 