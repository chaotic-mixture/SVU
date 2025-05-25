import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from models.database import (
    Item, Price, ExchangeRate, DataUpdateLog, SVUValue, MarketData,
    ItemType, DataSource, MarketType, init_db
)
from datetime import datetime, timedelta
import json
import logging
from typing import List, Dict, Any
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataInitializer:
    """数据初始化类"""
    
    def __init__(self, db_url: str = 'sqlite:///svu_data.db'):
        self.engine = init_db(db_url)
        
    def initialize_base_items(self):
        """初始化基础物品数据"""
        logger.info("开始初始化基础物品数据...")
        
        base_items = [
            # 主要货币
            {
                'name': 'US Dollar',
                'symbol': 'USD',
                'type': ItemType.CURRENCY.value,
                'market_type': MarketType.FOREX.value,
                'description': '美元',
                'metadata': {
                    'country': 'USA',
                    'is_major': True,
                    'currency_code': 'USD',
                    'decimal_places': 2
                }
            },
            {
                'name': 'Euro',
                'symbol': 'EUR',
                'type': ItemType.CURRENCY.value,
                'market_type': MarketType.FOREX.value,
                'description': '欧元',
                'metadata': {
                    'country': 'EU',
                    'is_major': True,
                    'currency_code': 'EUR',
                    'decimal_places': 2
                }
            },
            {
                'name': 'British Pound',
                'symbol': 'GBP',
                'type': ItemType.CURRENCY.value,
                'market_type': MarketType.FOREX.value,
                'description': '英镑',
                'metadata': {
                    'country': 'UK',
                    'is_major': True,
                    'currency_code': 'GBP',
                    'decimal_places': 2
                }
            },
            {
                'name': 'Japanese Yen',
                'symbol': 'JPY',
                'type': ItemType.CURRENCY.value,
                'market_type': MarketType.FOREX.value,
                'description': '日元',
                'metadata': {
                    'country': 'Japan',
                    'is_major': True,
                    'currency_code': 'JPY',
                    'decimal_places': 0
                }
            },
            {
                'name': 'Chinese Yuan',
                'symbol': 'CNY',
                'type': ItemType.CURRENCY.value,
                'market_type': MarketType.FOREX.value,
                'description': '人民币',
                'metadata': {
                    'country': 'China',
                    'is_major': True,
                    'currency_code': 'CNY',
                    'decimal_places': 2
                }
            },
            
            # 主要加密货币
            {
                'name': 'Bitcoin',
                'symbol': 'BTC',
                'type': ItemType.CRYPTO.value,
                'market_type': MarketType.CRYPTO.value,
                'description': '比特币',
                'metadata': {
                    'market_cap_rank': 1,
                    'max_supply': 21000000,
                    'algorithm': 'SHA-256',
                    'block_time': 600
                }
            },
            {
                'name': 'Ethereum',
                'symbol': 'ETH',
                'type': ItemType.CRYPTO.value,
                'market_type': MarketType.CRYPTO.value,
                'description': '以太坊',
                'metadata': {
                    'market_cap_rank': 2,
                    'max_supply': None,
                    'algorithm': 'Ethash',
                    'block_time': 15
                }
            },
            
            # 贵金属
            {
                'name': 'Gold',
                'symbol': 'XAU',
                'type': ItemType.PRECIOUS_METAL.value,
                'market_type': MarketType.COMMODITY.value,
                'description': '黄金',
                'metadata': {
                    'unit': 'troy ounce',
                    'purity': '99.99%',
                    'delivery_form': 'bullion'
                }
            },
            {
                'name': 'Silver',
                'symbol': 'XAG',
                'type': ItemType.PRECIOUS_METAL.value,
                'market_type': MarketType.COMMODITY.value,
                'description': '白银',
                'metadata': {
                    'unit': 'troy ounce',
                    'purity': '99.99%',
                    'delivery_form': 'bullion'
                }
            },
            
            # 大宗商品
            {
                'name': 'Crude Oil',
                'symbol': 'CL',
                'type': ItemType.COMMODITY.value,
                'market_type': MarketType.COMMODITY.value,
                'description': '原油',
                'metadata': {
                    'unit': 'barrel',
                    'grade': 'WTI',
                    'delivery_location': 'Cushing, Oklahoma'
                }
            },
            {
                'name': 'Natural Gas',
                'symbol': 'NG',
                'type': ItemType.COMMODITY.value,
                'market_type': MarketType.COMMODITY.value,
                'description': '天然气',
                'metadata': {
                    'unit': 'MMBtu',
                    'delivery_location': 'Henry Hub'
                }
            },
            
            # 主要股票指数
            {
                'name': 'S&P 500',
                'symbol': 'SPX',
                'type': ItemType.INDEX.value,
                'market_type': MarketType.STOCK.value,
                'description': '标普500指数',
                'metadata': {
                    'exchange': 'NYSE',
                    'calculation_method': 'market_cap_weighted',
                    'base_value': 100
                }
            },
            {
                'name': 'Dow Jones',
                'symbol': 'DJI',
                'type': ItemType.INDEX.value,
                'market_type': MarketType.STOCK.value,
                'description': '道琼斯工业平均指数',
                'metadata': {
                    'exchange': 'NYSE',
                    'calculation_method': 'price_weighted',
                    'base_value': 100
                }
            },
            
            # 主要股票
            {
                'name': 'Apple Inc.',
                'symbol': 'AAPL',
                'type': ItemType.STOCK.value,
                'market_type': MarketType.STOCK.value,
                'description': '苹果公司',
                'metadata': {
                    'exchange': 'NASDAQ',
                    'sector': 'Technology',
                    'industry': 'Consumer Electronics'
                }
            },
            {
                'name': 'Microsoft Corporation',
                'symbol': 'MSFT',
                'type': ItemType.STOCK.value,
                'market_type': MarketType.STOCK.value,
                'description': '微软公司',
                'metadata': {
                    'exchange': 'NASDAQ',
                    'sector': 'Technology',
                    'industry': 'Software'
                }
            },
            
            # SVU
            {
                'name': 'SVU',
                'symbol': 'SVU',
                'type': ItemType.SVU.value,
                'market_type': None,
                'description': '标准价值单位',
                'metadata': {
                    'base_value': 100,
                    'calculation_method': 'weighted_average'
                }
            }
        ]
        
        with Session(self.engine) as session:
            for item_data in base_items:
                # 检查是否已存在
                existing = session.query(Item).filter_by(symbol=item_data['symbol']).first()
                if not existing:
                    item = Item(**item_data)
                    session.add(item)
                    logger.info(f"添加物品: {item_data['symbol']}")
            
            session.commit()
            
    def initialize_historical_data(self):
        """初始化历史数据"""
        logger.info("开始初始化历史数据...")
        
        # 获取所有物品
        with Session(self.engine) as session:
            items = session.query(Item).all()
            
            # 生成过去一年的日期
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            dates = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # 为每个物品生成历史价格数据
            for item in items:
                if item.type == ItemType.SVU.value:
                    continue
                    
                # 根据物品类型生成不同的基础价格
                if item.type == ItemType.CURRENCY.value:
                    base_price = 100
                elif item.type == ItemType.CRYPTO.value:
                    base_price = 1000
                elif item.type == ItemType.PRECIOUS_METAL.value:
                    base_price = 2000
                elif item.type == ItemType.COMMODITY.value:
                    base_price = 50
                elif item.type == ItemType.STOCK.value:
                    base_price = 150
                elif item.type == ItemType.INDEX.value:
                    base_price = 3000
                else:
                    base_price = 100
                
                # 生成价格数据
                prices = [base_price * (1 + 0.1 * (i % 10) / 10) for i in range(len(dates))]
                
                # 添加价格数据
                for date, price in zip(dates, prices):
                    # 生成OHLC数据
                    daily_volatility = price * 0.02  # 2%的日波动率
                    open_price = price * (1 + 0.01 * (i % 5) / 5)
                    high_price = price + daily_volatility
                    low_price = price - daily_volatility
                    close_price = price * (1 - 0.01 * (i % 5) / 5)
                    
                    price_obj = Price(
                        item_id=item.id,
                        price=price,
                        open_price=open_price,
                        high_price=high_price,
                        low_price=low_price,
                        close_price=close_price,
                        volume=base_price * 1000 * (1 + 0.2 * (i % 10) / 10),
                        timestamp=date,
                        source=DataSource.CUSTOM.value,
                        confidence=0.9,
                        metadata={
                            'is_simulated': True,
                            'volatility': 0.02
                        }
                    )
                    session.add(price_obj)
                    
                # 添加市场数据
                if item.type in [ItemType.CRYPTO.value, ItemType.STOCK.value]:
                    market_data = MarketData(
                        item_id=item.id,
                        market_type=item.market_type,
                        timestamp=end_date,
                        volume_24h=base_price * 1000000,
                        market_cap=base_price * 1000000000,
                        circulating_supply=1000000000 if item.type == ItemType.CRYPTO.value else None,
                        total_supply=1000000000 if item.type == ItemType.CRYPTO.value else None,
                        max_supply=21000000 if item.symbol == 'BTC' else None,
                        source=DataSource.CUSTOM.value,
                        confidence=0.9,
                        metadata={
                            'is_simulated': True,
                            'data_type': 'market_data'
                        }
                    )
                    session.add(market_data)
                    
                logger.info(f"添加{item.symbol}的历史数据")
            
            # 生成汇率数据
            currencies = [item for item in items if item.type == ItemType.CURRENCY.value]
            for source in currencies:
                for target in currencies:
                    if source.id != target.id:
                        # 生成模拟汇率数据
                        base_rate = 1.0 if source.symbol == 'USD' else 0.8
                        rates = [base_rate * (1 + 0.05 * (i % 10) / 10) for i in range(len(dates))]
                        
                        for date, rate in zip(dates, rates):
                            rate_obj = ExchangeRate(
                                source_item_id=source.id,
                                target_item_id=target.id,
                                rate=rate,
                                timestamp=date,
                                source=DataSource.CUSTOM.value,
                                confidence=0.9,
                                metadata={
                                    'is_simulated': True,
                                    'pair': f"{source.symbol}/{target.symbol}"
                                }
                            )
                            session.add(rate_obj)
                            
                        logger.info(f"添加{source.symbol}/{target.symbol}的汇率数据")
            
            session.commit()
            
    def initialize_svu_values(self):
        """初始化SVU价值数据"""
        logger.info("开始初始化SVU价值数据...")
        
        with Session(self.engine) as session:
            # 获取SVU物品
            svu_item = session.query(Item).filter_by(symbol='SVU').first()
            if not svu_item:
                logger.error("未找到SVU物品")
                return
                
            # 获取所有非SVU物品
            items = session.query(Item).filter(Item.type != ItemType.SVU.value).all()
            
            # 生成过去一年的日期
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            dates = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # 为每个物品生成SVU价值数据
            for item in items:
                # 获取最新的价格数据
                latest_price = session.query(Price)\
                    .filter_by(item_id=item.id)\
                    .order_by(Price.timestamp.desc())\
                    .first()
                    
                if latest_price:
                    # 生成模拟SVU价值数据
                    base_value = 100  # SVU基准值
                    values = [base_value * (1 + 0.05 * (i % 10) / 10) for i in range(len(dates))]
                    
                    for date, value in zip(dates, values):
                        svu_value = SVUValue(
                            item_id=item.id,
                            svu_value=value,
                            timestamp=date,
                            confidence=0.9,
                            calculation_method='weighted_average',
                            metadata={
                                'is_simulated': True,
                                'price_id': latest_price.id,
                                'base_value': base_value
                            }
                        )
                        session.add(svu_value)
                        
                    logger.info(f"添加{item.symbol}的SVU价值数据")
            
            session.commit()
            
    def run(self):
        """运行初始化流程"""
        try:
            self.initialize_base_items()
            self.initialize_historical_data()
            self.initialize_svu_values()
            logger.info("数据初始化完成")
        except Exception as e:
            logger.error(f"数据初始化失败: {str(e)}")
            raise

if __name__ == '__main__':
    initializer = DataInitializer()
    initializer.run() 