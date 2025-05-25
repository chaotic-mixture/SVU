import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.visualizer import DataVisualizer
from utils.data_collector import DataCollector
from utils.data_processor import DataProcessor
import pandas as pd
from datetime import datetime, timedelta

def main():
    # 初始化数据采集器和处理器
    collector = DataCollector()
    processor = DataProcessor()
    visualizer = DataVisualizer()
    
    # 设置日期范围（最近30天）
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # 获取并处理数据
    print("正在获取数据...")
    
    # 黄金价格数据
    gold_data = collector.get_gold_price(
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )
    
    # 货币汇率数据
    currency_rates = collector.get_currency_rates()
    currency_data = pd.DataFrame({
        'currency': list(currency_rates.keys()),
        'rate': list(currency_rates.values()),
        'date': datetime.now()
    })
    
    # 加密货币价格数据
    crypto_prices = collector.get_crypto_prices()
    crypto_data = pd.DataFrame({
        'symbol': list(crypto_prices.keys()),
        'price': list(crypto_prices.values()),
        'date': datetime.now()
    })
    
    # 生成可视化图表
    print("\n正在生成图表...")
    
    # 黄金价格趋势图
    visualizer.plot_gold_price_trend(gold_data)
    
    # 货币汇率对比图
    visualizer.plot_currency_comparison(currency_data)
    
    # 加密货币价格对比图
    visualizer.plot_crypto_prices(crypto_data)
    
    # SVU价值对比图
    svu_data = {
        'gold': gold_data,
        'currencies': currency_data,
        'crypto': crypto_data
    }
    visualizer.plot_svu_comparison(svu_data)
    
    print("\n可视化完成！图表已保存到 visualization 目录")

if __name__ == "__main__":
    main() 