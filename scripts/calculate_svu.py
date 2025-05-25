import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.data_processor import DataProcessor
import pandas as pd
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def ensure_directory_exists(directory: str) -> None:
    """确保目录存在，如果不存在则创建
    
    Args:
        directory: 目录路径
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"创建目录: {directory}")

def validate_gold_price(price: float) -> bool:
    """验证黄金价格是否在合理范围内
    
    Args:
        price: 黄金价格
        
    Returns:
        bool: 价格是否合理
    """
    return 1000 <= price <= 3000  # 假设的合理价格范围

def main():
    try:
        # 确保数据目录存在
        ensure_directory_exists('data')
        
        # 初始化数据处理器
        processor = DataProcessor()
        
        # 获取当前时间
        current_time = datetime.now()
        
        # 示例：加载黄金价格数据
        gold_price = 1800.0  # 示例黄金价格
        if not validate_gold_price(gold_price):
            logger.warning(f"黄金价格 {gold_price} 可能不在合理范围内")
        
        gold_data = pd.DataFrame({
            'date': [current_time],
            'price': [gold_price]
        })
        
        # 保存数据
        output_file = 'data/gold_price.csv'
        gold_data.to_csv(output_file, index=False)
        logger.info(f"黄金价格数据已保存到: {output_file}")
        
        processor.load_gold_price(output_file)
        
        # 示例货币价格数据
        currency_prices = {
            'USD': 1.0,
            'EUR': 1.1,
            'GBP': 0.85,
            'JPY': 110.0,
            'CNY': 6.5
        }
        
        # 计算SVU值
        svu_values = processor.normalize_currency_values(currency_prices)
        
        # 输出结果
        print("\n=== SVU 价值计算结果 ===")
        print(f"基准黄金价格: ${processor.gold_price_usd}/盎司")
        print(f"SVU基准值: {processor.svu_base}")
        print("\n各货币SVU值:")
        for currency, value in svu_values.items():
            print(f"{currency}: {value:.2f} SVU")
        
        # 计算置信度
        confidence = processor.calculate_confidence_score(
            data_points=100,
            time_span=30
        )
        print(f"\n数据置信度: {confidence}")
        
        logger.info("SVU计算完成")
        
    except Exception as e:
        logger.error(f"发生错误: {str(e)}")
        raise

if __name__ == "__main__":
    main() 