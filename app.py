from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from sqlalchemy.orm import Session
from models.database import (
    Item, Price, ExchangeRate, DataUpdateLog, SVUValue, MarketData,
    ItemType, DataSource, MarketType, init_db, Base
)
from datetime import datetime, timedelta
import json
import os
import requests
import webbrowser
from threading import Timer
from typing import Dict, List, Optional, Tuple
import logging
from sqlalchemy import inspect
import random
import numpy as np
import sys
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Boolean, ForeignKey, JSON
from sqlalchemy import text
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.urandom(24)

# 数据库配置
DATABASE_URL = "sqlite:///svu.db"
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=3600)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# 定义模型
class Item(Base):
    __tablename__ = 'items'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    symbol = Column(String(20), unique=True, nullable=False)
    type = Column(String(20), nullable=False)
    market_type = Column(String(20), nullable=False)
    description = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    item_metadata = Column(JSON)

class Price(Base):
    __tablename__ = 'prices'
    
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('items.id'), nullable=False)
    price = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    volume = Column(Float)
    open_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    close_price = Column(Float)
    source = Column(String(50))
    confidence = Column(Float)
    price_metadata = Column(JSON)

class MarketData(Base):
    __tablename__ = 'market_data'
    
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('items.id'), nullable=False)
    market_type = Column(String(20), nullable=False)
    volume_24h = Column(Float)
    market_cap = Column(Float)
    circulating_supply = Column(Float)
    total_supply = Column(Float)
    max_supply = Column(Float)
    timestamp = Column(DateTime, nullable=False)
    source = Column(String(50))
    confidence = Column(Float)
    market_metadata = Column(JSON)

# 全局数据库会话
db_session = None

def get_session():
    """获取数据库会话"""
    return Session()

# 创建调度器
scheduler = BackgroundScheduler()

def update_all_prices():
    """定时更新所有价格数据"""
    try:
        with Session() as session:
            items = session.query(Item).all()
            for item in items:
                try:
                    is_valid, item_data, item_errors = validate_and_fetch_data(item.symbol, item.type)
                    if is_valid and 'price' in item_data and item_data['price'] > 0:
                        price = Price(
                            item_id=item.id,
                            price=float(item_data['price']),
                            timestamp=datetime.utcnow(),
                            source=item_data.get('source', 'API'),
                            confidence=1.0,
                            price_metadata=item_data
                        )
                        session.add(price)
                        logger.info(f"自动更新价格：{item.name} ({item.symbol}) - {item_data['price']}")
                except Exception as e:
                    logger.error(f"更新{item.symbol}价格失败: {str(e)}")
            session.commit()
    except Exception as e:
        logger.error(f"定时更新价格失败: {str(e)}")

def init_db():
    """初始化数据库"""
    try:
        logger.info("正在初始化数据库...")
        
        # 创建数据库表
        logger.info("正在创建数据库表...")
        Base.metadata.create_all(engine)
        logger.info("数据库表创建完成")
        
        # 初始化基础数据
        logger.info("正在初始化基础数据...")
        init_initial_data()
        logger.info("基础数据初始化完成")
        
        # 启动调度器（如果尚未启动）
        if not scheduler.running:
            scheduler.add_job(
                update_all_prices,
                IntervalTrigger(minutes=5),
                id='update_all_prices',
                replace_existing=True
            )
            scheduler.start()
            logger.info("价格更新调度器已启动")
        
        logger.info("数据库初始化完成")
    except Exception as e:
        logger.error(f"数据库初始化失败: {str(e)}")
        raise

# 预定义的基础数据
INITIAL_DATA = {
    'currencies': [
        {'symbol': 'USD', 'name': '美元', 'type': ItemType.CURRENCY.value, 'market_type': MarketType.FOREX.value, 'price': 1.0},
        {'symbol': 'EUR', 'name': '欧元', 'type': ItemType.CURRENCY.value, 'market_type': MarketType.FOREX.value, 'price': 1.08},
        {'symbol': 'GBP', 'name': '英镑', 'type': ItemType.CURRENCY.value, 'market_type': MarketType.FOREX.value, 'price': 1.26},
        {'symbol': 'JPY', 'name': '日元', 'type': ItemType.CURRENCY.value, 'market_type': MarketType.FOREX.value, 'price': 0.0067},
        {'symbol': 'CNY', 'name': '人民币', 'type': ItemType.CURRENCY.value, 'market_type': MarketType.FOREX.value, 'price': 0.14},
        {'symbol': 'AUD', 'name': '澳元', 'type': ItemType.CURRENCY.value, 'market_type': MarketType.FOREX.value, 'price': 0.66},
        {'symbol': 'CAD', 'name': '加元', 'type': ItemType.CURRENCY.value, 'market_type': MarketType.FOREX.value, 'price': 0.74},
        {'symbol': 'CHF', 'name': '瑞士法郎', 'type': ItemType.CURRENCY.value, 'market_type': MarketType.FOREX.value, 'price': 1.12},
        {'symbol': 'HKD', 'name': '港币', 'type': ItemType.CURRENCY.value, 'market_type': MarketType.FOREX.value, 'price': 0.13},
        {'symbol': 'SGD', 'name': '新加坡元', 'type': ItemType.CURRENCY.value, 'market_type': MarketType.FOREX.value, 'price': 0.75}
    ],
    'cryptos': [
        {'symbol': 'BTC', 'name': '比特币', 'type': ItemType.CRYPTO.value, 'market_type': MarketType.CRYPTO.value, 'price': 43000.0},
        {'symbol': 'ETH', 'name': '以太坊', 'type': ItemType.CRYPTO.value, 'market_type': MarketType.CRYPTO.value, 'price': 2300.0},
        {'symbol': 'USDT', 'name': '泰达币', 'type': ItemType.CRYPTO.value, 'market_type': MarketType.CRYPTO.value, 'price': 1.0},
        {'symbol': 'BNB', 'name': '币安币', 'type': ItemType.CRYPTO.value, 'market_type': MarketType.CRYPTO.value, 'price': 310.0},
        {'symbol': 'XRP', 'name': '瑞波币', 'type': ItemType.CRYPTO.value, 'market_type': MarketType.CRYPTO.value, 'price': 0.58}
    ],
    'commodities': [
        {'symbol': 'GOLD', 'name': '黄金', 'type': ItemType.COMMODITY.value, 'market_type': MarketType.COMMODITY.value, 'price': 2020.0},
        {'symbol': 'SILVER', 'name': '白银', 'type': ItemType.COMMODITY.value, 'market_type': MarketType.COMMODITY.value, 'price': 23.5},
        {'symbol': 'PLAT', 'name': '铂金', 'type': ItemType.COMMODITY.value, 'market_type': MarketType.COMMODITY.value, 'price': 890.0},
        {'symbol': 'PALL', 'name': '钯金', 'type': ItemType.COMMODITY.value, 'market_type': MarketType.COMMODITY.value, 'price': 980.0},
        {'symbol': 'OIL', 'name': '原油', 'type': ItemType.COMMODITY.value, 'market_type': MarketType.COMMODITY.value, 'price': 78.5}
    ]
}

def generate_historical_prices(base_price, reference_price, days=365, volatility=0.02):
    """生成相对于基准计价物的历史价格数据"""
    # 计算初始相对价格
    initial_relative_price = base_price / reference_price
    
    # 生成时间序列（根据时间周期调整采样频率）
    timestamps = []
    current_time = datetime.utcnow() - timedelta(days=days)
    
    # 根据天数调整采样频率
    if days <= 1:  # 1天
        interval = timedelta(hours=1)
        points = 24
    elif days <= 7:  # 1周
        interval = timedelta(hours=6)
        points = 28
    elif days <= 30:  # 1月
        interval = timedelta(days=1)
        points = 30
    elif days <= 90:  # 3月
        interval = timedelta(days=3)
        points = 30
    elif days <= 180:  # 6月
        interval = timedelta(days=7)
        points = 26
    else:  # 1年
        interval = timedelta(days=14)
        points = 26
    
    for _ in range(points):
        timestamps.append(current_time)
        current_time += interval
    
    # 使用更真实的随机游走模型
    relative_prices = [initial_relative_price]
    trend = np.random.normal(0, volatility * 0.1)  # 添加趋势
    for _ in range(len(timestamps) - 1):
        # 添加随机波动和趋势
        change = np.random.normal(trend, volatility)
        new_relative_price = relative_prices[-1] * (1 + change)
        # 确保相对价格不会太低
        new_relative_price = max(new_relative_price, initial_relative_price * 0.1)
        relative_prices.append(new_relative_price)
    
    # 生成数据点
    data_points = []
    for timestamp, relative_price in zip(timestamps, relative_prices):
        # 计算实际价格（相对价格 * 基准价格）
        actual_price = relative_price * reference_price
        
        # 生成更真实的OHLCV数据
        daily_volatility = volatility * 0.5
        open_price = actual_price * (1 + np.random.normal(0, daily_volatility))
        high_price = max(open_price, actual_price) * (1 + abs(np.random.normal(0, daily_volatility)))
        low_price = min(open_price, actual_price) * (1 - abs(np.random.normal(0, daily_volatility)))
        close_price = actual_price
        
        # 生成更真实的成交量
        base_volume = 1000000  # 基础成交量
        volume = base_volume * (1 + np.random.normal(0, 0.3))  # 添加随机波动
        volume = max(volume, base_volume * 0.1)  # 确保成交量不会太低
        
        data_points.append({
            'timestamp': timestamp,
            'price': actual_price,
            'relative_price': relative_price,
            'volume': volume,
            'open_price': open_price,
            'high_price': high_price,
            'low_price': low_price,
            'close_price': close_price,
            'source': 'SIMULATED',
            'confidence': 0.95
        })
    
    return data_points

def init_initial_data():
    """初始化基础数据"""
    session = Session()
    try:
        # 首先添加基准计价物（USD）
        base_currency = INITIAL_DATA['currencies'][0]  # USD
        
        # 检查USD是否已存在
        existing_base = session.query(Item).filter_by(symbol=base_currency['symbol']).first()
        if not existing_base:
            base_item = Item(
                name=base_currency['name'],
                symbol=base_currency['symbol'],
                type=base_currency['type'],
                market_type=base_currency['market_type']
            )
            session.add(base_item)
            session.flush()
            
            # 为基准计价物添加固定价格
            base_price = Price(
                item_id=base_item.id,
                price=base_currency['price'],
                timestamp=datetime.utcnow(),
                source='BASE',
                confidence=1.0
            )
            session.add(base_price)
        
        # 添加其他货币
        for currency in INITIAL_DATA['currencies'][1:]:  # 跳过USD
            # 检查货币是否已存在
            existing = session.query(Item).filter_by(symbol=currency['symbol']).first()
            if not existing:
                item = Item(
                    name=currency['name'],
                    symbol=currency['symbol'],
                    type=currency['type'],
                    market_type=currency['market_type']
                )
                session.add(item)
                session.flush()
                
                # 添加初始价格
                price = Price(
                    item_id=item.id,
                    price=currency['price'],
                    timestamp=datetime.utcnow(),
                    source='BASE',
                    confidence=1.0
                )
                session.add(price)
                
                # 生成相对于USD的历史价格数据
                historical_prices = generate_historical_prices(
                    currency['price'],
                    base_currency['price'],
                    days=365,
                    volatility=0.001  # 货币波动较小
                )
                
                # 添加历史价格数据
                for price_data in historical_prices:
                    price = Price(
                        item_id=item.id,
                        price=price_data['price'],
                        timestamp=price_data['timestamp'],
                        volume=price_data['volume'],
                        open_price=price_data['open_price'],
                        high_price=price_data['high_price'],
                        low_price=price_data['low_price'],
                        close_price=price_data['close_price'],
                        source=price_data['source'],
                        confidence=price_data['confidence'],
                        price_metadata={'relative_price': price_data['relative_price']}
                    )
                    session.add(price)
        
        # 添加加密货币
        for crypto in INITIAL_DATA['cryptos']:
            # 检查加密货币是否已存在
            existing = session.query(Item).filter_by(symbol=crypto['symbol']).first()
            if not existing:
                item = Item(
                    name=crypto['name'],
                    symbol=crypto['symbol'],
                    type=crypto['type'],
                    market_type=crypto['market_type']
                )
                session.add(item)
                session.flush()
                
                # 添加初始价格
                price = Price(
                    item_id=item.id,
                    price=crypto['price'],
                    timestamp=datetime.utcnow(),
                    source='BASE',
                    confidence=1.0
                )
                session.add(price)
                
                # 生成相对于USD的历史价格数据
                historical_prices = generate_historical_prices(
                    crypto['price'],
                    base_currency['price'],
                    days=365,
                    volatility=0.03  # 加密货币波动较大
                )
                
                # 添加历史价格数据
                for price_data in historical_prices:
                    price = Price(
                        item_id=item.id,
                        price=price_data['price'],
                        timestamp=price_data['timestamp'],
                        volume=price_data['volume'],
                        open_price=price_data['open_price'],
                        high_price=price_data['high_price'],
                        low_price=price_data['low_price'],
                        close_price=price_data['close_price'],
                        source=price_data['source'],
                        confidence=price_data['confidence'],
                        price_metadata={'relative_price': price_data['relative_price']}
                    )
                    session.add(price)
                
                # 添加市场数据
                market_data = MarketData(
                    item_id=item.id,
                    market_type=item.market_type,
                    volume_24h=random.uniform(1000000, 100000000),
                    market_cap=random.uniform(10000000, 1000000000),
                    circulating_supply=random.uniform(1000000, 100000000),
                    total_supply=random.uniform(1000000, 100000000),
                    max_supply=random.uniform(1000000, 100000000),
                    timestamp=datetime.utcnow(),
                    source='SIMULATED',
                    confidence=0.95
                )
                session.add(market_data)
        
        # 添加商品
        for commodity in INITIAL_DATA['commodities']:
            # 检查商品是否已存在
            existing = session.query(Item).filter_by(symbol=commodity['symbol']).first()
            if not existing:
                item = Item(
                    name=commodity['name'],
                    symbol=commodity['symbol'],
                    type=commodity['type'],
                    market_type=commodity['market_type']
                )
                session.add(item)
                session.flush()
                
                # 添加初始价格
                price = Price(
                    item_id=item.id,
                    price=commodity['price'],
                    timestamp=datetime.utcnow(),
                    source='BASE',
                    confidence=1.0
                )
                session.add(price)
                
                # 生成相对于USD的历史价格数据
                historical_prices = generate_historical_prices(
                    commodity['price'],
                    base_currency['price'],
                    days=365,
                    volatility=0.02  # 商品波动适中
                )
                
                # 添加历史价格数据
                for price_data in historical_prices:
                    price = Price(
                        item_id=item.id,
                        price=price_data['price'],
                        timestamp=price_data['timestamp'],
                        volume=price_data['volume'],
                        open_price=price_data['open_price'],
                        high_price=price_data['high_price'],
                        low_price=price_data['low_price'],
                        close_price=price_data['close_price'],
                        source=price_data['source'],
                        confidence=price_data['confidence'],
                        price_metadata={'relative_price': price_data['relative_price']}
                    )
                    session.add(price)
        
        session.commit()
        logger.info("基础数据初始化完成")
    except Exception as e:
        session.rollback()
        logger.error(f"初始化基础数据失败: {str(e)}")
        raise
    finally:
        session.close()

# API配置
API_KEYS = {
    'ALPHA_VANTAGE': os.getenv('ALPHA_VANTAGE_API_KEY', ''),
    'COINGECKO': os.getenv('COINGECKO_API_KEY', ''),
    'EXCHANGE_RATE': os.getenv('EXCHANGE_RATE_API_KEY', '')
}

def open_browser():
    """在默认浏览器中打开应用"""
    webbrowser.open('http://127.0.0.1:5000/dashboard')

def validate_and_fetch_data(symbol: str, item_type: str) -> Tuple[bool, Dict, List[str]]:
    """验证并获取数据"""
    errors = []
    data = {}
    
    try:
        # 检查数据库中是否已存在
        with Session() as session:
            existing_item = session.query(Item).filter_by(symbol=symbol).first()
            if existing_item:
                # 获取最新价格
                latest_price = session.query(Price)\
                    .filter_by(item_id=existing_item.id)\
                    .order_by(Price.timestamp.desc())\
                    .first()
                
                return True, {
                    'name': existing_item.name,
                    'symbol': existing_item.symbol,
                    'type': existing_item.type,
                    'market_type': existing_item.market_type,
                    'price': latest_price.price if latest_price else 0.0
                }, []
        
        # 验证类型
        if item_type not in [t.value for t in ItemType]:
            errors.append('无效的物品类型')
            return False, data, errors
            
        # 根据类型获取数据
        if item_type == ItemType.CURRENCY.value:
            # 验证货币代码格式
            if not symbol.isalpha() or len(symbol) != 3:
                errors.append('无效的货币代码格式')
                return False, data, errors
                
            # 从汇率API获取数据
            response = requests.get(
                f'https://api.exchangerate-api.com/v4/latest/USD',
                params={'api_key': API_KEYS['EXCHANGE_RATE']}
            )
            if response.status_code == 200:
                rates = response.json()['rates']
                if symbol in rates:
                    data = {
                        'name': get_currency_name(symbol),
                        'symbol': symbol,
                        'type': ItemType.CURRENCY.value,
                        'market_type': MarketType.FOREX.value,
                        'price': rates[symbol]
                    }
                else:
                    errors.append('无法获取该货币的汇率数据')
            else:
                errors.append('无法获取货币数据')
                
        elif item_type == ItemType.CRYPTO.value:
            # 从CoinGecko获取数据
            response = requests.get(
                f'https://api.coingecko.com/api/v3/simple/price',
                params={
                    'ids': symbol.lower(),
                    'vs_currencies': 'usd',
                    'include_market_cap': 'true',
                    'include_24hr_vol': 'true',
                    'include_24hr_change': 'true',
                    'include_last_updated_at': 'true'
                }
            )
            if response.status_code == 200:
                data = response.json()
                if symbol.lower() in data:
                    data = data[symbol.lower()]
                    data['name'] = get_crypto_name(symbol)
                    data['symbol'] = symbol
                    data['type'] = ItemType.CRYPTO.value
                    data['market_type'] = MarketType.CRYPTO.value
                else:
                    errors.append('无法找到该加密货币')
            else:
                errors.append('无法获取加密货币数据')
                
        elif item_type == ItemType.COMMODITY.value:
            # 从Alpha Vantage获取数据
            response = requests.get(
                'https://www.alphavantage.co/query',
                params={
                    'function': 'GLOBAL_QUOTE',
                    'symbol': symbol,
                    'apikey': API_KEYS['ALPHA_VANTAGE']
                }
            )
            if response.status_code == 200:
                data = response.json()
                if 'Global Quote' in data:
                    quote = data['Global Quote']
                    data = {
                        'name': get_commodity_name(symbol),
                        'symbol': symbol,
                        'type': ItemType.COMMODITY.value,
                        'market_type': MarketType.COMMODITY.value,
                        'price': float(quote['05. price'])
                    }
                else:
                    errors.append('无法找到该商品')
            else:
                errors.append('无法获取商品数据')
                
        return len(errors) == 0, data, errors
        
    except Exception as e:
        errors.append(f'数据获取失败: {str(e)}')
        return False, data, errors

def get_currency_name(symbol: str) -> str:
    """获取货币名称"""
    currency_names = {
        'USD': '美元',
        'EUR': '欧元',
        'GBP': '英镑',
        'JPY': '日元',
        'CNY': '人民币',
        'AUD': '澳元',
        'CAD': '加元',
        'CHF': '瑞士法郎',
        'HKD': '港币',
        'SGD': '新加坡元'
    }
    return currency_names.get(symbol, symbol)

def get_crypto_name(symbol: str) -> str:
    """获取加密货币名称"""
    crypto_names = {
        'BTC': '比特币',
        'ETH': '以太坊',
        'USDT': '泰达币',
        'BNB': '币安币',
        'XRP': '瑞波币',
        'ADA': '卡尔达诺',
        'SOL': '索拉纳',
        'DOT': '波卡',
        'DOGE': '狗狗币',
        'AVAX': '雪崩币'
    }
    return crypto_names.get(symbol.upper(), symbol)

def get_commodity_name(symbol: str) -> str:
    """获取商品名称"""
    commodity_names = {
        'GOLD': '黄金',
        'SILVER': '白银',
        'PLAT': '铂金',
        'PALL': '钯金',
        'OIL': '原油',
        'NATURAL_GAS': '天然气',
        'COPPER': '铜',
        'ALUMINUM': '铝',
        'IRON': '铁矿石',
        'CORN': '玉米'
    }
    return commodity_names.get(symbol.upper(), symbol)

@app.route('/')
def index():
    """首页路由"""
    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    """仪表板页面"""
    try:
        with get_session() as session:
            # 获取所有物品
            items = session.query(Item).filter_by(is_active=True).all()
            
            # 获取最新的价格数据
            latest_prices = {}
            for item in items:
                latest_price = session.query(Price)\
                    .filter_by(item_id=item.id)\
                    .order_by(Price.timestamp.desc())\
                    .first()
                if latest_price:
                    latest_prices[item.symbol] = latest_price.price
            
            # 统计信息
            stats = {
                'currency': session.query(Item).filter_by(type='currency').count(),
                'crypto': session.query(Item).filter_by(type='crypto').count(),
                'commodity': session.query(Item).filter_by(type='commodity').count(),
                'stock': session.query(Item).filter_by(type='stock').count()
            }
            
            return render_template('dashboard.html',
                                items=items,
                                latest_prices=latest_prices,
                                stats=stats)
    except Exception as e:
        logger.error(f"访问仪表板时出错: {str(e)}")
        return str(e), 500

@app.route('/api/items', methods=['GET'])
def get_items():
    """获取所有物品"""
    with Session() as session:
        items = session.query(Item).all()
        return jsonify([{
            'id': item.id,
            'name': item.name,
            'symbol': item.symbol,
            'type': item.type,
            'market_type': item.market_type,
            'description': item.description,
            'is_active': item.is_active,
            'item_metadata': item.item_metadata
        } for item in items])

@app.route('/api/validate', methods=['POST'])
def validate_item():
    """验证物品数据"""
    data = request.json
    symbol = data.get('symbol', '').strip().upper()
    item_type = data.get('type')
    
    if not symbol or not item_type:
        return jsonify({'error': ['请提供物品代码和类型']}), 400
        
    is_valid, item_data, errors = validate_and_fetch_data(symbol, item_type)
    
    if not is_valid:
        return jsonify({'error': errors}), 400
        
    return jsonify({
        'is_valid': True,
        'data': item_data
    })

@app.route('/api/items', methods=['POST'])
def add_item():
    """添加新物品"""
    data = request.json
    symbol = data.get('symbol', '').strip().upper()
    item_type = data.get('type')
    
    if not symbol or not item_type:
        return jsonify({'error': ['请提供物品代码和类型']}), 400
        
    is_valid, item_data, errors = validate_and_fetch_data(symbol, item_type)
    
    if not is_valid:
        return jsonify({'error': errors}), 400
        
    try:
        with Session() as session:
            # 检查是否已存在
            existing = session.query(Item).filter_by(symbol=symbol).first()
            if existing:
                return jsonify({'error': ['该物品已存在']}), 400
                
            # 创建新物品
            item = Item(
                name=item_data.get('name', symbol),
                symbol=symbol,
                type=item_type,
                market_type=item_data.get('market_type'),
                description=data.get('description'),
                item_metadata=item_data
            )
            session.add(item)
            
            # 添加价格数据
            if 'price' in item_data:
                price = Price(
                    item_id=item.id,
                    price=float(item_data['price']),
                    timestamp=datetime.utcnow(),
                    source=item_data.get('source', 'API'),
                    confidence=1.0,
                    price_metadata=item_data
                )
                session.add(price)
                
            session.commit()
            
            return jsonify({
                'id': item.id,
                'name': item.name,
                'symbol': item.symbol,
                'type': item.type
            })
    except Exception as e:
        return jsonify({'error': [str(e)]}), 500

@app.route('/api/items/<int:item_id>', methods=['DELETE'])
def delete_item(item_id):
    """删除物品"""
    try:
        with Session() as session:
            item = session.query(Item).get(item_id)
            if not item:
                return jsonify({'error': ['物品不存在']}), 404
                
            session.delete(item)
            session.commit()
            return jsonify({'message': '删除成功'})
    except Exception as e:
        return jsonify({'error': [str(e)]}), 500

@app.route('/api/items/<int:item_id>/prices', methods=['GET'])
def get_item_prices(item_id):
    """获取物品价格历史"""
    with Session() as session:
        prices = session.query(Price)\
            .filter_by(item_id=item_id)\
            .order_by(Price.timestamp.desc())\
            .limit(100)\
            .all()
            
        return jsonify([{
            'id': price.id,
            'price': price.price,
            'volume': price.volume,
            'open_price': price.open_price,
            'high_price': price.high_price,
            'low_price': price.low_price,
            'close_price': price.close_price,
            'timestamp': price.timestamp.isoformat(),
            'source': price.source,
            'confidence': price.confidence,
            'price_metadata': price.price_metadata
        } for price in prices])

@app.route('/api/items/<int:item_id>/market-data', methods=['GET'])
def get_item_market_data(item_id):
    """获取物品市场数据"""
    with Session() as session:
        market_data = session.query(MarketData)\
            .filter_by(item_id=item_id)\
            .order_by(MarketData.timestamp.desc())\
            .limit(100)\
            .all()
            
        return jsonify([{
            'id': data.id,
            'market_type': data.market_type,
            'volume_24h': data.volume_24h,
            'market_cap': data.market_cap,
            'circulating_supply': data.circulating_supply,
            'total_supply': data.total_supply,
            'max_supply': data.max_supply,
            'timestamp': data.timestamp.isoformat(),
            'source': data.source,
            'confidence': data.confidence,
            'market_metadata': data.market_metadata
        } for data in market_data])

@app.route('/api/items/<int:item_id>/svu-values', methods=['GET'])
def get_item_svu_values(item_id):
    """获取物品SVU值"""
    with Session() as session:
        svu_values = session.query(SVUValue)\
            .filter_by(item_id=item_id)\
            .order_by(SVUValue.timestamp.desc())\
            .limit(100)\
            .all()
            
        return jsonify([{
            'id': value.id,
            'svu_value': value.svu_value,
            'timestamp': value.timestamp.isoformat(),
            'confidence': value.confidence,
            'calculation_method': value.calculation_method,
            'svu_metadata': value.svu_metadata
        } for value in svu_values])

@app.route('/api/items/<int:item_id>/update-price', methods=['POST'])
def update_item_price(item_id):
    """更新物品价格"""
    try:
        with Session() as session:
            item = session.query(Item).get(item_id)
            if not item:
                return jsonify({'error': ['物品不存在']}), 404
                
            # 根据类型获取最新数据
            is_valid, item_data, errors = validate_and_fetch_data(item.symbol, item.type)
            
            if not is_valid:
                return jsonify({'error': errors}), 400
                
            # 添加新价格记录
            if 'price' in item_data:
                price = Price(
                    item_id=item.id,
                    price=float(item_data['price']),
                    timestamp=datetime.utcnow(),
                    source=item_data.get('source', 'API'),
                    confidence=1.0,
                    price_metadata=item_data
                )
                session.add(price)
                
                # 添加市场数据
                if item.type in [ItemType.CRYPTO.value, ItemType.STOCK.value]:
                    market_data = MarketData(
                        item_id=item.id,
                        market_type=item.market_type,
                        volume_24h=item_data.get('volume_24h'),
                        market_cap=item_data.get('market_cap'),
                        circulating_supply=item_data.get('circulating_supply'),
                        total_supply=item_data.get('total_supply'),
                        max_supply=item_data.get('max_supply'),
                        timestamp=datetime.utcnow(),
                        source=item_data.get('source', 'API'),
                        confidence=1.0,
                        market_metadata=item_data
                    )
                    session.add(market_data)
                
                session.commit()
                
                return jsonify({
                    'id': item.id,
                    'name': item.name,
                    'symbol': item.symbol,
                    'price': float(item_data['price'])
                })
            else:
                return jsonify({'error': ['无法获取价格数据']}), 400
                
    except Exception as e:
        return jsonify({'error': [str(e)]}), 500

@app.route('/api/items/batch-update', methods=['POST'])
def batch_update_prices():
    """批量更新所有物品的价格（仅限管理员）"""
    try:
        # 检查管理员权限
        if not request.headers.get('X-Admin-Token') == os.getenv('ADMIN_TOKEN'):
            return jsonify({'error': ['无权限执行此操作']}), 403
            
        with Session() as session:
            items = session.query(Item).all()
            updated_count = 0
            errors = []
            
            for item in items:
                try:
                    is_valid, item_data, item_errors = validate_and_fetch_data(item.symbol, item.type)
                    
                    if is_valid and 'price' in item_data and item_data['price'] > 0:
                        price = Price(
                            item_id=item.id,
                            price=float(item_data['price']),
                            timestamp=datetime.utcnow(),
                            source=item_data.get('source', 'API'),
                            confidence=1.0,
                            price_metadata=item_data
                        )
                        session.add(price)
                        updated_count += 1
                        logger.info(f"手动更新价格：{item.name} ({item.symbol}) - {item_data['price']}")
                    else:
                        error_msg = f"{item.symbol}: {', '.join(item_errors)}" if item_errors else f"{item.symbol}: 无法获取价格数据"
                        errors.append(error_msg)
                        logger.warning(error_msg)
                except Exception as e:
                    error_msg = f"{item.symbol}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
            
            session.commit()
            
            return jsonify({
                'message': f'成功更新 {updated_count} 个物品的价格',
                'errors': errors
            })
            
    except Exception as e:
        return jsonify({'error': [str(e)]}), 500

@app.route('/api/exchange-rate', methods=['GET'])
def get_exchange_rate():
    """获取汇率数据"""
    session = Session()
    try:
        from_symbol = request.args.get('from', 'USD')  # 默认基准货币为USD
        to_symbol = request.args.get('to')
        amount = float(request.args.get('amount', 1))
        
        if not to_symbol:
            return jsonify({'error': '缺少目标货币参数'}), 400
            
        # 获取源货币和目标货币
        from_item = session.query(Item).filter_by(symbol=from_symbol).first()
        to_item = session.query(Item).filter_by(symbol=to_symbol).first()
        
        if not from_item or not to_item:
            return jsonify({'error': '找不到指定的货币'}), 404
            
        # 获取最新价格
        from_price = session.query(Price).filter_by(item_id=from_item.id).order_by(Price.timestamp.desc()).first()
        to_price = session.query(Price).filter_by(item_id=to_item.id).order_by(Price.timestamp.desc()).first()
        
        if not from_price or not to_price:
            return jsonify({'error': '没有可用的价格数据'}), 404
            
        # 计算汇率（基准货币价格/目标货币价格）
        # 例如：如果 GBP 价格是 1.26 USD，那么 1 USD = 1/1.26 GBP
        rate = 1 / (to_price.price / from_price.price)
        result = amount * rate
        
        return jsonify({
            'success': True,
            'data': {
                'from': from_symbol,
                'to': to_symbol,
                'rate': rate,
                'amount': amount,
                'result': result,
                'timestamp': from_price.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            }
        })
    except ValueError:
        return jsonify({'error': '无效的金额'}), 400
    except Exception as e:
        logger.error(f"计算汇率时出错: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/items/<symbol>/price-history', methods=['GET'])
def get_price_history(symbol):
    """获取价格历史数据"""
    try:
        period = request.args.get('period', '1d')
        base_symbol = request.args.get('base', 'USD')  # 默认基准货币为USD
        
        with Session() as session:
            # 获取目标物品
            target_item = session.query(Item).filter_by(symbol=symbol).first()
            if not target_item:
                return jsonify({'error': f'未找到物品: {symbol}'}), 404
                
            # 获取基准物品
            base_item = session.query(Item).filter_by(symbol=base_symbol).first()
            if not base_item:
                return jsonify({'error': f'未找到基准物品: {base_symbol}'}), 404
                
            # 设置时间范围
            now = datetime.utcnow()
            if period == '1d':
                start_time = now - timedelta(days=1)
                interval = timedelta(minutes=15)  # 15分钟间隔
                max_points = 96  # 24小时 * 4点/小时
                time_format = '%H:%M'
            elif period == '1w':
                start_time = now - timedelta(weeks=1)
                interval = timedelta(hours=2)  # 2小时间隔
                max_points = 84  # 7天 * 12点/天
                time_format = '%m-%d %H:%M'
            elif period == '1m':
                start_time = now - timedelta(days=30)
                interval = timedelta(hours=4)  # 4小时间隔
                max_points = 180  # 30天 * 6点/天
                time_format = '%m-%d'
            elif period == '3m':
                start_time = now - timedelta(days=90)
                interval = timedelta(hours=12)  # 12小时间隔
                max_points = 180  # 90天 * 2点/天
                time_format = '%m-%d'
            elif period == '6m':
                start_time = now - timedelta(days=180)
                interval = timedelta(days=1)  # 1天间隔
                max_points = 180  # 180天 * 1点/天
                time_format = '%m-%d'
            elif period == '1y':
                start_time = now - timedelta(days=365)
                interval = timedelta(days=2)  # 2天间隔
                max_points = 182  # 365天 * 0.5点/天
                time_format = '%Y-%m-%d'
            elif period == '5y':
                start_time = now - timedelta(days=365*5)
                interval = timedelta(days=7)  # 7天间隔
                max_points = 260  # 5年*52周
                time_format = '%Y-%m'
            else:
                return jsonify({'error': '无效的时间周期'}), 400
                
            # 获取价格数据
            target_prices = session.query(Price)\
                .filter(
                    Price.item_id == target_item.id,
                    Price.timestamp >= start_time
                )\
                .order_by(Price.timestamp.asc())\
                .all()
                
            base_prices = session.query(Price)\
                .filter(
                    Price.item_id == base_item.id,
                    Price.timestamp >= start_time
                )\
                .order_by(Price.timestamp.asc())\
                .all()
                
            if not target_prices:
                logger.warning(f"未找到{symbol}在{start_time}之后的价格数据")
                return jsonify({'error': f'未找到{symbol}的价格数据'}), 404
                
            if not base_prices:
                logger.warning(f"未找到{base_symbol}在{start_time}之后的价格数据")
                return jsonify({'error': f'未找到{base_symbol}的价格数据'}), 404
                
            # 创建基准价格映射
            base_price_map = {p.timestamp: p.price for p in base_prices}
            
            # 计算相对价格
            data_points = []
            current_time = start_time
            
            while current_time <= now:
                # 找到最接近的基准价格时间点
                closest_base_time = min(base_price_map.keys(), key=lambda x: abs((x - current_time).total_seconds()))
                closest_target_time = min(target_prices, key=lambda x: abs((x.timestamp - current_time).total_seconds()))
                
                if abs((closest_base_time - current_time).total_seconds()) <= interval.total_seconds() and \
                   abs((closest_target_time.timestamp - current_time).total_seconds()) <= interval.total_seconds():
                    # 修正：目标价格/基准价格
                    relative_price = closest_target_time.price / base_price_map[closest_base_time]
                    data_points.append({
                        'timestamp': current_time,
                        'price': relative_price,
                        'volume': closest_target_time.volume if hasattr(closest_target_time, 'volume') else None
                    })
                
                current_time += interval
                
            # 如果数据点太少，使用所有可用数据点
            if len(data_points) < 5:
                logger.warning(f"数据点数量不足，使用所有可用数据点")
                data_points = []
                for tp in target_prices:
                    closest_base_time = min(base_price_map.keys(), key=lambda x: abs((x - tp.timestamp).total_seconds()))
                    if abs((closest_base_time - tp.timestamp).total_seconds()) <= interval.total_seconds():
                        # 修正：目标价格/基准价格
                        relative_price = tp.price / base_price_map[closest_base_time]
                        data_points.append({
                            'timestamp': tp.timestamp,
                            'price': relative_price,
                            'volume': tp.volume if hasattr(tp, 'volume') else None
                        })
            
            # 按时间排序
            data_points.sort(key=lambda x: x['timestamp'])
            
            # 采样数据点
            if len(data_points) > max_points:
                # 使用等间隔采样
                step = len(data_points) // max_points
                sampled_points = []
                for i in range(0, len(data_points), step):
                    sampled_points.append(data_points[i])
                data_points = sampled_points
                
            # 准备返回数据
            labels = [dp['timestamp'].strftime(time_format) for dp in data_points]
            prices = [dp['price'] for dp in data_points]
            volumes = [dp['volume'] for dp in data_points]
            
            # 计算统计数据
            if prices:
                current_price = prices[-1]
                first_price = prices[0]
                change_24h = ((current_price - first_price) / first_price) * 100
                high = max(prices)
                low = min(prices)
            else:
                current_price = 0
                change_24h = 0
                high = 0
                low = 0
                
            stats = {
                'current': current_price,
                'change_24h': change_24h,
                'high': high,
                'low': low
            }
            
            return jsonify({
                'success': True,
                'data': {
                    'labels': labels,
                    'prices': prices,
                    'volumes': volumes,
                    'stats': stats,
                    'base_symbol': base_symbol,
                    'symbol': symbol,
                    'period': period
                }
            })
    except Exception as e:
        logger.error(f"获取历史价格数据时出错: {str(e)}", exc_info=True)
        return jsonify({'error': f'获取历史价格数据失败: {str(e)}'}), 500

# 在应用启动时初始化数据库
@app.before_first_request
def before_first_request():
    """在第一个请求之前初始化数据库"""
    try:
        init_db()
    except Exception as e:
        logger.error(f"应用初始化失败: {str(e)}")
        raise

# 在应用关闭时清理资源
@app.teardown_appcontext
def shutdown_scheduler(exception=None):
    """在应用关闭时停止调度器"""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("价格更新调度器已停止")

if __name__ == '__main__':
    try:
        # 初始化数据库
        init_db()
        # 设置定时器，在应用启动后1.5秒打开浏览器
        Timer(1.5, open_browser).start()
        # 使用host='127.0.0.1'而不是默认的'localhost'
        app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=True)
    except Exception as e:
        logger.error(f"服务器启动失败: {str(e)}")
        sys.exit(1) 