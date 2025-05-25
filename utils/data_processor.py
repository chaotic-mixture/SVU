import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime

class DataProcessor:
    """数据处理工具类"""
    
    def __init__(self):
        self.gold_price_usd = None  # 黄金价格（美元/盎司）
        self.svu_base = 100  # SVU基准值
        
    def load_gold_price(self, file_path: str) -> None:
        """加载黄金价格数据
        
        Args:
            file_path: 黄金价格数据文件路径
        """
        try:
            df = pd.read_csv(file_path)
            self.gold_price_usd = df['price'].iloc[-1]  # 获取最新价格
        except Exception as e:
            print(f"加载黄金价格数据失败: {str(e)}")
            
    def calculate_svu_ratio(self, currency_price: float) -> float:
        """计算货币相对于SVU的比率
        
        Args:
            currency_price: 货币价格（美元）
            
        Returns:
            float: 货币相对于SVU的比率
        """
        if self.gold_price_usd is None:
            raise ValueError("请先加载黄金价格数据")
            
        return (currency_price / self.gold_price_usd) * self.svu_base
    
    def normalize_currency_values(self, 
                                currency_prices: Dict[str, float]) -> Dict[str, float]:
        """将货币价格标准化为SVU值
        
        Args:
            currency_prices: 货币价格字典 {货币代码: 美元价格}
            
        Returns:
            Dict[str, float]: 标准化后的SVU值字典
        """
        if self.gold_price_usd is None:
            raise ValueError("请先加载黄金价格数据")
            
        return {
            currency: self.calculate_svu_ratio(price)
            for currency, price in currency_prices.items()
        }
    
    def calculate_confidence_score(self, 
                                 data_points: int,
                                 time_span: int) -> float:
        """计算数据置信度分数
        
        Args:
            data_points: 数据点数量
            time_span: 时间跨度（天）
            
        Returns:
            float: 置信度分数 (0-1)
        """
        # 简单的置信度计算示例
        coverage_score = min(data_points / (time_span * 2), 1.0)
        return round(coverage_score, 2) 