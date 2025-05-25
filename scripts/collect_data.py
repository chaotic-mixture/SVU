import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.data_collector import DataCollector
from datetime import datetime, timedelta
import pandas as pd

def main():
    # 初始化数据采集器
    collector = DataCollector()
    
    # 设置日期范围（最近30天）
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # 获取黄金价格数据
    print("正在获取黄金价格数据...")
    gold_data = collector.get_gold_price(
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )
    collector.save_data(gold_data, 'gold_prices.csv')
    
    # 获取货币汇率数据
    print("\n正在获取货币汇率数据...")
    currency_rates = collector.get_currency_rates()
    currency_data = pd.DataFrame({
        'currency': list(currency_rates.keys()),
        'rate': list(currency_rates.values()),
        'date': datetime.now()
    })
    collector.save_data(currency_data, 'currency_rates.csv')
    
    # 获取加密货币价格
    print("\n正在获取加密货币价格...")
    crypto_prices = collector.get_crypto_prices()
    crypto_data = pd.DataFrame({
        'symbol': list(crypto_prices.keys()),
        'price': list(crypto_prices.values()),
        'date': datetime.now()
    })
    collector.save_data(crypto_data, 'crypto_prices.csv')
    
    print("\n数据采集完成！")

if __name__ == "__main__":
    main() 