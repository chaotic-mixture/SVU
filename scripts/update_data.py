import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from models.database import (
    Item, Price, ExchangeRate, DataUpdateLog, SVUValue, MarketData,
    ItemType, DataSource, MarketType, init_db
)
from datetime import datetime, timedelta
import logging
import schedule
import time
from typing import List, Dict, Any
import pandas as pd
import numpy as np
from api.worldbank import WorldBankAPI
from api.alphavantage import AlphaVantageAPI
from api.coingecko import CoinGeckoAPI
from api.imf import IMFAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataUpdater:
    """数据更新类"""
    
    def __init__(self, db_url: str = 'sqlite:///svu_data.db'):
        self.engine = init_db(db_url)
        self.worldbank_api = WorldBankAPI()
        self.alphavantage_api = AlphaVantageAPI()
        self.coingecko_api = CoinGeckoAPI()
        self.imf_api = IMFAPI()
        
    def update_currency_data(self):
        """更新货币数据"""
        logger.info("开始更新货币数据...")
        
        try:
            with Session(self.engine) as session:
                # 获取所有货币
                currencies = session.query(Item).filter_by(type=ItemType.CURRENCY.value).all()
                
                # 从IMF获取汇率数据
                rates_data = self.imf_api.get_exchange_rates()
                
                for currency in currencies:
                    if currency.symbol in rates_data:
                        rate = rates_data[currency.symbol]
                        
                        # 添加价格数据
                        price = Price(
                            item_id=currency.id,
                            price=rate,
                            timestamp=datetime.now(),
                            source=DataSource.IMF.value,
                            confidence=0.95,
                            metadata={
                                'base_currency': 'USD',
                                'rate_type': 'spot'
                            }
                        )
                        session.add(price)
                        
                        # 记录更新日志
                        log = DataUpdateLog(
                            item_id=currency.id,
                            update_type='price',
                            status='success',
                            timestamp=datetime.now(),
                            source=DataSource.IMF.value,
                            metadata={
                                'rate': rate,
                                'base_currency': 'USD'
                            }
                        )
                        session.add(log)
                        
                        logger.info(f"更新{currency.symbol}汇率: {rate}")
                
                session.commit()
                logger.info("货币数据更新完成")
                
        except Exception as e:
            logger.error(f"更新货币数据失败: {str(e)}")
            raise
            
    def update_crypto_data(self):
        """更新加密货币数据"""
        logger.info("开始更新加密货币数据...")
        
        try:
            with Session(self.engine) as session:
                # 获取所有加密货币
                cryptos = session.query(Item).filter_by(type=ItemType.CRYPTO.value).all()
                
                # 从CoinGecko获取数据
                for crypto in cryptos:
                    data = self.coingecko_api.get_crypto_data(crypto.symbol.lower())
                    
                    if data:
                        # 添加价格数据
                        price = Price(
                            item_id=crypto.id,
                            price=data['price'],
                            open_price=data['open'],
                            high_price=data['high'],
                            low_price=data['low'],
                            close_price=data['close'],
                            volume=data['volume'],
                            timestamp=datetime.now(),
                            source=DataSource.COINGECKO.value,
                            confidence=0.95,
                            metadata={
                                'market_cap': data['market_cap'],
                                'price_change_24h': data['price_change_24h']
                            }
                        )
                        session.add(price)
                        
                        # 添加市场数据
                        market_data = MarketData(
                            item_id=crypto.id,
                            market_type=MarketType.CRYPTO.value,
                            timestamp=datetime.now(),
                            volume_24h=data['volume'],
                            market_cap=data['market_cap'],
                            circulating_supply=data['circulating_supply'],
                            total_supply=data['total_supply'],
                            max_supply=data['max_supply'],
                            source=DataSource.COINGECKO.value,
                            confidence=0.95,
                            metadata={
                                'price_change_24h': data['price_change_24h'],
                                'market_cap_rank': data['market_cap_rank']
                            }
                        )
                        session.add(market_data)
                        
                        # 记录更新日志
                        log = DataUpdateLog(
                            item_id=crypto.id,
                            update_type='price_and_market',
                            status='success',
                            timestamp=datetime.now(),
                            source=DataSource.COINGECKO.value,
                            metadata={
                                'price': data['price'],
                                'volume': data['volume'],
                                'market_cap': data['market_cap']
                            }
                        )
                        session.add(log)
                        
                        logger.info(f"更新{crypto.symbol}数据: 价格={data['price']}, 市值={data['market_cap']}")
                
                session.commit()
                logger.info("加密货币数据更新完成")
                
        except Exception as e:
            logger.error(f"更新加密货币数据失败: {str(e)}")
            raise
            
    def update_commodity_data(self):
        """更新大宗商品数据"""
        logger.info("开始更新大宗商品数据...")
        
        try:
            with Session(self.engine) as session:
                # 获取所有大宗商品
                commodities = session.query(Item).filter_by(type=ItemType.COMMODITY.value).all()
                
                # 从Alpha Vantage获取数据
                for commodity in commodities:
                    data = self.alphavantage_api.get_commodity_data(commodity.symbol)
                    
                    if data:
                        # 添加价格数据
                        price = Price(
                            item_id=commodity.id,
                            price=data['price'],
                            open_price=data['open'],
                            high_price=data['high'],
                            low_price=data['low'],
                            close_price=data['close'],
                            volume=data['volume'],
                            timestamp=datetime.now(),
                            source=DataSource.ALPHA_VANTAGE.value,
                            confidence=0.95,
                            metadata={
                                'unit': data['unit'],
                                'exchange': data['exchange']
                            }
                        )
                        session.add(price)
                        
                        # 记录更新日志
                        log = DataUpdateLog(
                            item_id=commodity.id,
                            update_type='price',
                            status='success',
                            timestamp=datetime.now(),
                            source=DataSource.ALPHA_VANTAGE.value,
                            metadata={
                                'price': data['price'],
                                'unit': data['unit']
                            }
                        )
                        session.add(log)
                        
                        logger.info(f"更新{commodity.symbol}数据: 价格={data['price']} {data['unit']}")
                
                session.commit()
                logger.info("大宗商品数据更新完成")
                
        except Exception as e:
            logger.error(f"更新大宗商品数据失败: {str(e)}")
            raise
            
    def update_stock_data(self):
        """更新股票数据"""
        logger.info("开始更新股票数据...")
        
        try:
            with Session(self.engine) as session:
                # 获取所有股票
                stocks = session.query(Item).filter_by(type=ItemType.STOCK.value).all()
                
                # 从Alpha Vantage获取数据
                for stock in stocks:
                    data = self.alphavantage_api.get_stock_data(stock.symbol)
                    
                    if data:
                        # 添加价格数据
                        price = Price(
                            item_id=stock.id,
                            price=data['price'],
                            open_price=data['open'],
                            high_price=data['high'],
                            low_price=data['low'],
                            close_price=data['close'],
                            volume=data['volume'],
                            timestamp=datetime.now(),
                            source=DataSource.ALPHA_VANTAGE.value,
                            confidence=0.95,
                            metadata={
                                'pe_ratio': data['pe_ratio'],
                                'dividend_yield': data['dividend_yield']
                            }
                        )
                        session.add(price)
                        
                        # 添加市场数据
                        market_data = MarketData(
                            item_id=stock.id,
                            market_type=MarketType.STOCK.value,
                            timestamp=datetime.now(),
                            volume_24h=data['volume'],
                            market_cap=data['market_cap'],
                            source=DataSource.ALPHA_VANTAGE.value,
                            confidence=0.95,
                            metadata={
                                'pe_ratio': data['pe_ratio'],
                                'dividend_yield': data['dividend_yield'],
                                'sector': data['sector']
                            }
                        )
                        session.add(market_data)
                        
                        # 记录更新日志
                        log = DataUpdateLog(
                            item_id=stock.id,
                            update_type='price_and_market',
                            status='success',
                            timestamp=datetime.now(),
                            source=DataSource.ALPHA_VANTAGE.value,
                            metadata={
                                'price': data['price'],
                                'volume': data['volume'],
                                'market_cap': data['market_cap']
                            }
                        )
                        session.add(log)
                        
                        logger.info(f"更新{stock.symbol}数据: 价格={data['price']}, 市值={data['market_cap']}")
                
                session.commit()
                logger.info("股票数据更新完成")
                
        except Exception as e:
            logger.error(f"更新股票数据失败: {str(e)}")
            raise
            
    def update_svu_values(self):
        """更新SVU价值数据"""
        logger.info("开始更新SVU价值数据...")
        
        try:
            with Session(self.engine) as session:
                # 获取所有非SVU物品
                items = session.query(Item).filter(Item.type != ItemType.SVU.value).all()
                
                for item in items:
                    # 获取最新的价格数据
                    latest_price = session.query(Price)\
                        .filter_by(item_id=item.id)\
                        .order_by(Price.timestamp.desc())\
                        .first()
                        
                    if latest_price:
                        # 计算SVU价值
                        svu_value = self.calculate_svu_value(item, latest_price)
                        
                        # 添加SVU价值数据
                        value = SVUValue(
                            item_id=item.id,
                            svu_value=svu_value,
                            timestamp=datetime.now(),
                            confidence=0.9,
                            calculation_method='weighted_average',
                            metadata={
                                'price_id': latest_price.id,
                                'base_value': 100
                            }
                        )
                        session.add(value)
                        
                        # 记录更新日志
                        log = DataUpdateLog(
                            item_id=item.id,
                            update_type='svu_value',
                            status='success',
                            timestamp=datetime.now(),
                            source=DataSource.CUSTOM.value,
                            metadata={
                                'svu_value': svu_value,
                                'price_id': latest_price.id
                            }
                        )
                        session.add(log)
                        
                        logger.info(f"更新{item.symbol}的SVU价值: {svu_value}")
                
                session.commit()
                logger.info("SVU价值数据更新完成")
                
        except Exception as e:
            logger.error(f"更新SVU价值数据失败: {str(e)}")
            raise
            
    def calculate_svu_value(self, item: Item, price: Price) -> float:
        """计算SVU价值"""
        # 这里实现SVU价值计算逻辑
        # 目前使用简单的加权平均方法
        base_value = 100  # SVU基准值
        
        if item.type == ItemType.CURRENCY.value:
            # 货币的SVU价值基于汇率
            return base_value * price.price
        elif item.type == ItemType.CRYPTO.value:
            # 加密货币的SVU价值基于市值
            market_data = session.query(MarketData)\
                .filter_by(item_id=item.id)\
                .order_by(MarketData.timestamp.desc())\
                .first()
            if market_data and market_data.market_cap:
                return base_value * (price.price / market_data.market_cap)
        elif item.type == ItemType.STOCK.value:
            # 股票的SVU价值基于市盈率
            if price.metadata and 'pe_ratio' in price.metadata:
                return base_value * (price.price / price.metadata['pe_ratio'])
        elif item.type == ItemType.COMMODITY.value:
            # 大宗商品的SVU价值基于历史价格
            historical_prices = session.query(Price)\
                .filter_by(item_id=item.id)\
                .order_by(Price.timestamp.desc())\
                .limit(30)\
                .all()
            if historical_prices:
                avg_price = sum(p.price for p in historical_prices) / len(historical_prices)
                return base_value * (price.price / avg_price)
                
        # 默认返回基准值
        return base_value
        
    def schedule_updates(self):
        """设置定时更新"""
        # 货币数据每小时更新
        schedule.every().hour.do(self.update_currency_data)
        
        # 加密货币数据每5分钟更新
        schedule.every(5).minutes.do(self.update_crypto_data)
        
        # 大宗商品数据每小时更新
        schedule.every().hour.do(self.update_commodity_data)
        
        # 股票数据每15分钟更新
        schedule.every(15).minutes.do(self.update_stock_data)
        
        # SVU价值数据每小时更新
        schedule.every().hour.do(self.update_svu_values)
        
        logger.info("定时更新已设置")
        
        while True:
            schedule.run_pending()
            time.sleep(1)
            
    def run_once(self):
        """运行一次更新"""
        try:
            self.update_currency_data()
            self.update_crypto_data()
            self.update_commodity_data()
            self.update_stock_data()
            self.update_svu_values()
            logger.info("数据更新完成")
        except Exception as e:
            logger.error(f"数据更新失败: {str(e)}")
            raise

if __name__ == '__main__':
    updater = DataUpdater()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--schedule':
        updater.schedule_updates()
    else:
        updater.run_once() 