from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Table, Boolean, Text, Index, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import enum

Base = declarative_base()

class DataSource(enum.Enum):
    IMF = "IMF"
    WORLD_BANK = "World Bank"
    FRED = "FRED"
    ALPHA_VANTAGE = "Alpha Vantage"
    COINGECKO = "CoinGecko"
    LBMA = "LBMA"
    YAHOO_FINANCE = "Yahoo Finance"
    BLOOMBERG = "Bloomberg"
    CUSTOM = "Custom"

class ItemType(enum.Enum):
    # 法定货币
    CURRENCY = "currency"
    # 加密货币
    CRYPTO = "crypto"
    # 贵金属
    PRECIOUS_METAL = "precious_metal"
    # 大宗商品
    COMMODITY = "commodity"
    # 股票
    STOCK = "stock"
    # 债券
    BOND = "bond"
    # 指数
    INDEX = "index"
    # 基金
    FUND = "fund"
    # 衍生品
    DERIVATIVE = "derivative"
    # 房地产
    REAL_ESTATE = "real_estate"
    # 艺术品
    ART = "art"
    # 收藏品
    COLLECTIBLE = "collectible"
    # SVU
    SVU = "svu"
    # 其他
    OTHER = "other"

class MarketType(enum.Enum):
    FOREX = "forex"  # 外汇市场
    CRYPTO = "crypto"  # 加密货币市场
    COMMODITY = "commodity"  # 商品市场
    STOCK = "stock"  # 股票市场
    BOND = "bond"  # 债券市场
    DERIVATIVE = "derivative"  # 衍生品市场
    REAL_ESTATE = "real_estate"  # 房地产市场
    ART = "art"  # 艺术品市场
    COLLECTIBLE = "collectible"  # 收藏品市场
    OTHER = "other"  # 其他市场

# 可计价物品之间的关联表
item_relationships = Table('item_relationships', Base.metadata,
    Column('source_id', Integer, ForeignKey('items.id')),
    Column('target_id', Integer, ForeignKey('items.id')),
    Column('relationship_type', String(50)),
    Column('weight', Float, default=1.0),
    Column('market_type', String(50)),
    Column('created_at', DateTime, default=datetime.utcnow),
    Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
)

class Item(Base):
    """可计价物品表"""
    __tablename__ = 'items'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    symbol = Column(String(20), unique=True, nullable=False)
    type = Column(String(50), nullable=False)
    market_type = Column(String(50))  # 市场类型
    description = Column(Text)
    is_active = Column(Boolean, default=True)
    item_metadata = Column(JSON)  # 重命名为item_metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    prices = relationship("Price", back_populates="item")
    source_relationships = relationship("Item",
        secondary=item_relationships,
        primaryjoin=id==item_relationships.c.source_id,
        secondaryjoin=id==item_relationships.c.target_id,
        backref="target_relationships"
    )
    
    # 索引
    __table_args__ = (
        Index('idx_items_symbol', 'symbol'),
        Index('idx_items_type', 'type'),
        Index('idx_items_market_type', 'market_type'),
    )

class Price(Base):
    """价格数据表"""
    __tablename__ = 'prices'
    
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('items.id'), nullable=False)
    price = Column(Float, nullable=False)
    volume = Column(Float)  # 交易量
    open_price = Column(Float)  # 开盘价
    high_price = Column(Float)  # 最高价
    low_price = Column(Float)  # 最低价
    close_price = Column(Float)  # 收盘价
    timestamp = Column(DateTime, nullable=False)
    source = Column(String(100))
    confidence = Column(Float)  # 数据置信度 (0-1)
    price_metadata = Column(JSON)  # 重命名为price_metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # 关系
    item = relationship("Item", back_populates="prices")
    
    # 索引
    __table_args__ = (
        Index('idx_prices_item_timestamp', 'item_id', 'timestamp'),
        Index('idx_prices_timestamp', 'timestamp'),
    )

class ExchangeRate(Base):
    """汇率数据表"""
    __tablename__ = 'exchange_rates'
    
    id = Column(Integer, primary_key=True)
    source_item_id = Column(Integer, ForeignKey('items.id'), nullable=False)
    target_item_id = Column(Integer, ForeignKey('items.id'), nullable=False)
    rate = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    source = Column(String(100))
    confidence = Column(Float)
    rate_metadata = Column(JSON)  # 重命名为rate_metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # 索引
    __table_args__ = (
        Index('idx_exchange_rates_timestamp', 'timestamp'),
        Index('idx_exchange_rates_pair', 'source_item_id', 'target_item_id'),
    )

class MarketData(Base):
    """市场数据表"""
    __tablename__ = 'market_data'
    
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('items.id'), nullable=False)
    market_type = Column(String(50), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    volume_24h = Column(Float)  # 24小时交易量
    market_cap = Column(Float)  # 市值
    circulating_supply = Column(Float)  # 流通量
    total_supply = Column(Float)  # 总供应量
    max_supply = Column(Float)  # 最大供应量
    source = Column(String(100))
    confidence = Column(Float)
    market_metadata = Column(JSON)  # 重命名为market_metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # 索引
    __table_args__ = (
        Index('idx_market_data_item_timestamp', 'item_id', 'timestamp'),
        Index('idx_market_data_market_type', 'market_type'),
    )

class DataUpdateLog(Base):
    """数据更新日志表"""
    __tablename__ = 'data_update_logs'
    
    id = Column(Integer, primary_key=True)
    data_type = Column(String(50), nullable=False)  # 'price', 'exchange_rate'等
    source = Column(String(100), nullable=False)
    status = Column(String(20), nullable=False)  # 'success', 'failed', 'partial'
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime)
    records_processed = Column(Integer, default=0)
    error_message = Column(Text)
    log_metadata = Column(JSON)  # 重命名为log_metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # 索引
    __table_args__ = (
        Index('idx_update_logs_timestamp', 'start_time'),
        Index('idx_update_logs_source', 'source'),
    )

class SVUValue(Base):
    """SVU价值表"""
    __tablename__ = 'svu_values'
    
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('items.id'), nullable=False)
    svu_value = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    confidence = Column(Float)
    calculation_method = Column(String(50))  # 计算方法
    svu_metadata = Column(JSON)  # 重命名为svu_metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # 索引
    __table_args__ = (
        Index('idx_svu_values_timestamp', 'timestamp'),
        Index('idx_svu_values_item', 'item_id'),
    )

def init_db(db_url: str = 'sqlite:///svu_data.db'):
    """初始化数据库
    
    Args:
        db_url: 数据库URL
    """
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return engine 