import requests
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from models.database import Item, Price, ExchangeRate, init_db
import logging

logger = logging.getLogger(__name__)

class DataCollector:
    """数据采集类，用于从多个数据源获取数据"""
    
    def __init__(self, db_url: str = 'sqlite:///svu_data.db'):
        load_dotenv()  # 加载环境变量
        self.api_keys = {
            'fred': os.getenv('FRED_API_KEY'),
            'coingecko': os.getenv('COINGECKO_API_KEY'),
            'alpha_vantage': os.getenv('ALPHA_VANTAGE_API_KEY'),
            'yahoo_finance': os.getenv('YAHOO_FINANCE_API_KEY')
        }
        self.engine = init_db(db_url)
        
    def initialize_data(self):
        """初始化基础数据"""
        logger.info("开始初始化基础数据...")
        
        # 添加基础货币
        currencies = [
            ('US Dollar', 'USD', 'currency', '美元'),
            ('Euro', 'EUR', 'currency', '欧元'),
            ('British Pound', 'GBP', 'currency', '英镑'),
            ('Japanese Yen', 'JPY', 'currency', '日元'),
            ('Chinese Yuan', 'CNY', 'currency', '人民币')
        ]
        
        # 添加加密货币
        cryptos = [
            ('Bitcoin', 'BTC', 'crypto', '比特币'),
            ('Ethereum', 'ETH', 'crypto', '以太坊')
        ]
        
        # 添加商品
        commodities = [
            ('Gold', 'XAU', 'commodity', '黄金'),
            ('Silver', 'XAG', 'commodity', '白银')
        ]
        
        # 添加SVU
        svu = [('SVU', 'SVU', 'svu', 'SVU价值单位')]
        
        # 添加所有物品
        all_items = currencies + cryptos + commodities + svu
        for name, symbol, type_, desc in all_items:
            try:
                self.add_item(name, symbol, type_, desc)
                logger.info(f"添加物品: {symbol}")
            except Exception as e:
                logger.error(f"添加物品{symbol}时出错: {str(e)}")
        
        # 添加一些示例价格数据
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        # 获取并保存黄金价格
        try:
            gold_data = self.get_gold_price(
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            logger.info(f"添加了{len(gold_data)}条黄金价格数据")
        except Exception as e:
            logger.error(f"获取黄金价格时出错: {str(e)}")
        
        # 获取并保存货币汇率
        try:
            rates = self.get_currency_rates()
            logger.info(f"添加了{len(rates)}个货币汇率")
        except Exception as e:
            logger.error(f"获取货币汇率时出错: {str(e)}")
        
        # 获取并保存加密货币价格
        try:
            crypto_prices = self.get_crypto_prices()
            logger.info(f"添加了{len(crypto_prices)}个加密货币价格")
        except Exception as e:
            logger.error(f"获取加密货币价格时出错: {str(e)}")
        
        logger.info("基础数据初始化完成")
        
    def add_item(self, name: str, symbol: str, type: str, description: str = "") -> Item:
        """添加新的可计价物品
        
        Args:
            name: 物品名称
            symbol: 物品代码
            type: 物品类型
            description: 物品描述
            
        Returns:
            Item: 新创建的物品对象
        """
        with Session(self.engine) as session:
            # 检查是否已存在
            existing = session.query(Item).filter_by(symbol=symbol).first()
            if existing:
                return existing
                
            item = Item(
                name=name,
                symbol=symbol,
                type=type,
                description=description
            )
            session.add(item)
            session.commit()
            return item
            
    def add_price(self, item_id: int, price: float, source: str, confidence: float = 1.0) -> Price:
        """添加价格数据
        
        Args:
            item_id: 物品ID
            price: 价格
            source: 数据来源
            confidence: 数据置信度
            
        Returns:
            Price: 新创建的价格对象
        """
        with Session(self.engine) as session:
            price_obj = Price(
                item_id=item_id,
                price=price,
                timestamp=datetime.utcnow(),
                source=source,
                confidence=confidence
            )
            session.add(price_obj)
            session.commit()
            return price_obj
            
    def add_exchange_rate(self, source_id: int, target_id: int, rate: float, 
                         source: str, confidence: float = 1.0) -> ExchangeRate:
        """添加汇率数据
        
        Args:
            source_id: 源物品ID
            target_id: 目标物品ID
            rate: 汇率
            source: 数据来源
            confidence: 数据置信度
            
        Returns:
            ExchangeRate: 新创建的汇率对象
        """
        with Session(self.engine) as session:
            rate_obj = ExchangeRate(
                source_item_id=source_id,
                target_item_id=target_id,
                rate=rate,
                timestamp=datetime.utcnow(),
                source=source,
                confidence=confidence
            )
            session.add(rate_obj)
            session.commit()
            return rate_obj
            
    def get_gold_price(self, start_date: str, end_date: str) -> pd.DataFrame:
        """从LBMA获取黄金价格数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: 包含日期和价格的DataFrame
        """
        # TODO: 实现真实的API调用
        dates = pd.date_range(start=start_date, end=end_date)
        prices = [1800.0 + i * 0.1 for i in range(len(dates))]
        
        df = pd.DataFrame({
            'date': dates,
            'price': prices
        })
        
        # 保存到数据库
        with Session(self.engine) as session:
            gold_item = session.query(Item).filter_by(symbol='XAU').first()
            if not gold_item:
                gold_item = self.add_item('Gold', 'XAU', 'commodity', '黄金')
            
            for _, row in df.iterrows():
                self.add_price(gold_item.id, row['price'], 'LBMA')
        
        return df
    
    def get_currency_rates(self, 
                          base_currency: str = 'USD',
                          currencies: List[str] = None) -> Dict[str, float]:
        """获取货币汇率数据
        
        Args:
            base_currency: 基准货币代码
            currencies: 目标货币代码列表
            
        Returns:
            Dict[str, float]: 货币汇率字典
        """
        if currencies is None:
            currencies = ['EUR', 'GBP', 'JPY', 'CNY']
            
        # TODO: 实现真实的API调用
        rates = {
            'EUR': 1.1,
            'GBP': 0.85,
            'JPY': 110.0,
            'CNY': 6.5
        }
        
        # 保存到数据库
        with Session(self.engine) as session:
            base_item = session.query(Item).filter_by(symbol=base_currency).first()
            if not base_item:
                base_item = self.add_item(base_currency, base_currency, 'currency')
            
            for curr in currencies:
                target_item = session.query(Item).filter_by(symbol=curr).first()
                if not target_item:
                    target_item = self.add_item(curr, curr, 'currency')
                
                self.add_exchange_rate(base_item.id, target_item.id, 
                                     rates.get(curr, 1.0), 'Alpha Vantage')
        
        return {curr: rates.get(curr, 1.0) for curr in currencies}
    
    def get_crypto_prices(self, 
                         symbols: List[str] = None) -> Dict[str, float]:
        """获取加密货币价格
        
        Args:
            symbols: 加密货币代码列表
            
        Returns:
            Dict[str, float]: 加密货币价格字典
        """
        if symbols is None:
            symbols = ['BTC', 'ETH']
            
        # TODO: 实现真实的API调用
        prices = {
            'BTC': 50000.0,
            'ETH': 3000.0
        }
        
        # 保存到数据库
        with Session(self.engine) as session:
            for symbol in symbols:
                crypto_item = session.query(Item).filter_by(symbol=symbol).first()
                if not crypto_item:
                    crypto_item = self.add_item(symbol, symbol, 'crypto')
                
                self.add_price(crypto_item.id, prices.get(symbol, 0.0), 'CoinGecko')
        
        return {symbol: prices.get(symbol, 0.0) for symbol in symbols}
    
    def get_historical_data(self, 
                           item_symbol: str,
                           start_date: str,
                           end_date: str) -> pd.DataFrame:
        """获取历史数据
        
        Args:
            item_symbol: 物品代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            pd.DataFrame: 历史数据DataFrame
        """
        with Session(self.engine) as session:
            item = session.query(Item).filter_by(symbol=item_symbol).first()
            if not item:
                raise ValueError(f"未找到物品: {item_symbol}")
            
            prices = session.query(Price).filter(
                Price.item_id == item.id,
                Price.timestamp >= start_date,
                Price.timestamp <= end_date
            ).all()
            
            if not prices:
                logger.warning(f"未找到{item_symbol}在{start_date}到{end_date}之间的价格数据")
                return pd.DataFrame()
            
            return pd.DataFrame([{
                'date': p.timestamp,
                'price': p.price,
                'source': p.source,
                'confidence': p.confidence
            } for p in prices])
    
    def get_relationship_data(self, 
                            item_symbol: str,
                            relationship_type: str = None) -> List[Tuple[str, str, float]]:
        """获取物品关系数据
        
        Args:
            item_symbol: 物品代码
            relationship_type: 关系类型
            
        Returns:
            List[Tuple[str, str, float]]: 关系数据列表
        """
        with Session(self.engine) as session:
            item = session.query(Item).filter_by(symbol=item_symbol).first()
            if not item:
                raise ValueError(f"未找到物品: {item_symbol}")
            
            rates = session.query(ExchangeRate).filter(
                ExchangeRate.source_item_id == item.id
            ).all()
            
            if not rates:
                logger.warning(f"未找到{item_symbol}的关系数据")
                return []
            
            return [(item.symbol, 
                    session.query(Item).get(r.target_item_id).symbol,
                    r.rate) for r in rates]
    
    def save_data(self, 
                 data: pd.DataFrame,
                 filename: str,
                 directory: str = 'data') -> None:
        """保存数据到CSV文件
        
        Args:
            data: 要保存的数据
            filename: 文件名
            directory: 保存目录
        """
        os.makedirs(directory, exist_ok=True)
        filepath = os.path.join(directory, filename)
        data.to_csv(filepath, index=False)
        logger.info(f"数据已保存到: {filepath}") 