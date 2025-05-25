import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.svu_model import SVUGraphModel, SVUPredictor
from utils.data_collector import DataCollector
from utils.data_processor import DataProcessor
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import networkx as nx
from typing import Dict, Tuple, List
from sqlalchemy.orm import Session
from models.database import Item, Price, ExchangeRate
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_graph_data(features: np.ndarray,
                     edge_index: np.ndarray,
                     window_size: int = 5) -> Tuple[np.ndarray, np.ndarray]:
    """创建图数据
    
    Args:
        features: 特征矩阵
        edge_index: 边索引矩阵
        window_size: 时间窗口大小
        
    Returns:
        Tuple[np.ndarray, np.ndarray]: 节点特征和边索引
    """
    if len(features) == 0:
        raise ValueError("特征矩阵为空")
        
    n_samples = len(features) - window_size + 1
    if n_samples <= 0:
        raise ValueError(f"时间窗口大小({window_size})大于特征数量({len(features)})")
        
    n_features = features.shape[1]
    logger.info(f"创建图数据: {n_samples}个样本, {n_features}个特征")
    
    # 创建节点特征
    node_features = []
    for i in range(n_samples):
        window = features[i:i+window_size]
        node_features.append(window.flatten())
    node_features = np.array(node_features)
    
    # 使用提供的边索引
    if len(edge_index) > 0:
        edge_index = edge_index[:, edge_index[0] < n_samples]
        edge_index = edge_index[:, edge_index[1] < n_samples]
    else:
        # 如果没有边索引，创建时间序列连接
        edge_index = np.array([[i, i+1] for i in range(n_samples-1)]).T
    
    # 添加自环
    self_loops = np.array([[i, i] for i in range(n_samples)]).T
    edge_index = np.concatenate([edge_index, self_loops], axis=1)
    
    logger.info(f"创建了{len(edge_index[0])}条边")
    return node_features, edge_index

def prepare_training_data(collector: DataCollector,
                         processor: DataProcessor,
                         start_date: str,
                         end_date: str) -> Dict[str, np.ndarray]:
    """准备训练数据
    
    Args:
        collector: 数据采集器
        processor: 数据处理器
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        Dict[str, np.ndarray]: 训练数据
    """
    logger.info(f"准备训练数据: {start_date} 到 {end_date}")
    
    # 从数据库获取所有物品
    with Session(collector.engine) as session:
        items = session.query(Item).all()
        if not items:
            raise ValueError("数据库中没有物品数据")
            
        logger.info(f"找到{len(items)}个物品")
        
        # 获取每个物品的历史价格数据
        features = []
        valid_items = []
        for item in items:
            try:
                hist_data = collector.get_historical_data(item.symbol, start_date, end_date)
                if not hist_data.empty:
                    features.append(hist_data['price'].values)
                    valid_items.append(item)
                    logger.info(f"获取到{item.symbol}的历史数据: {len(hist_data)}条记录")
                else:
                    logger.warning(f"{item.symbol}没有历史数据")
            except Exception as e:
                logger.error(f"获取{item.symbol}历史数据时出错: {str(e)}")
        
        if not features:
            raise ValueError("没有找到任何有效的历史数据")
            
        # 将所有特征对齐到相同的时间点
        min_length = min(len(f) for f in features)
        logger.info(f"所有特征对齐到{min_length}个时间点")
        
        features = np.array([f[:min_length] for f in features]).T
        
        # 获取物品之间的关系数据
        edge_index = []
        for item in valid_items:
            try:
                relationships = collector.get_relationship_data(item.symbol)
                for source, target, rate in relationships:
                    source_idx = next(i for i, it in enumerate(valid_items) if it.symbol == source)
                    target_idx = next(i for i, it in enumerate(valid_items) if it.symbol == target)
                    edge_index.append([source_idx, target_idx])
                logger.info(f"获取到{item.symbol}的{len(relationships)}个关系")
            except Exception as e:
                logger.error(f"获取{item.symbol}关系数据时出错: {str(e)}")
        
        edge_index = np.array(edge_index).T if edge_index else np.array([[], []])
    
    # 标准化特征
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    # 创建图数据
    node_features, edge_index = create_graph_data(features_scaled, edge_index)
    
    # 准备标签（使用下一个时间点的SVU价格作为标签）
    try:
        with Session(collector.engine) as session:
            svu_item = session.query(Item).filter_by(symbol='SVU').first()
            if svu_item:
                svu_data = collector.get_historical_data('SVU', start_date, end_date)
                if not svu_data.empty:
                    labels = svu_data['price'].values[1:min_length]  # 使用下一个时间点的价格作为标签
                    logger.info("使用SVU价格作为标签")
                else:
                    labels = features[1:, 0]
                    logger.info("使用第一个物品的价格作为标签")
            else:
                labels = features[1:, 0]
                logger.info("使用第一个物品的价格作为标签")
    except Exception as e:
        logger.error(f"准备标签时出错: {str(e)}")
        labels = features[1:, 0]
        logger.info("使用第一个物品的价格作为标签")
    
    return {
        'features': node_features,
        'edge_index': edge_index,
        'labels': labels
    }

def split_data(data: Dict[str, np.ndarray],
               test_size: float = 0.2) -> Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]:
    """划分训练集和测试集
    
    Args:
        data: 数据字典
        test_size: 测试集比例
        
    Returns:
        Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]: 训练集和测试集
    """
    # 获取数据长度
    n_samples = len(data['features'])
    indices = np.arange(n_samples)
    
    # 划分索引
    train_indices, test_indices = train_test_split(
        indices,
        test_size=test_size,
        shuffle=False
    )
    
    logger.info(f"划分数据集: {len(train_indices)}个训练样本, {len(test_indices)}个测试样本")
    
    # 划分数据
    train_data = {
        'features': data['features'][train_indices],
        'edge_index': data['edge_index'],
        'labels': data['labels'][train_indices]
    }
    
    test_data = {
        'features': data['features'][test_indices],
        'edge_index': data['edge_index'],
        'labels': data['labels'][test_indices]
    }
    
    return train_data, test_data

def main():
    try:
        # 初始化组件
        collector = DataCollector()
        processor = DataProcessor()
        
        # 初始化数据
        logger.info("初始化数据...")
        collector.initialize_data()
        
        # 设置日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)  # 使用一年的数据
        
        # 准备数据
        logger.info("开始准备训练数据...")
        data = prepare_training_data(
            collector,
            processor,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        # 划分训练集和测试集
        train_data, test_data = split_data(data, test_size=0.2)
        
        # 创建和训练模型
        logger.info("开始训练模型...")
        model = SVUGraphModel(
            input_dim=data['features'].shape[1],  # 动态特征维度
            hidden_dim=64,
            output_dim=1
        )
        predictor = SVUPredictor(model)
        
        # 训练模型
        losses = predictor.train(
            train_data,
            epochs=100,
            lr=0.001
        )
        
        # 保存模型
        os.makedirs('models/saved', exist_ok=True)
        predictor.save_model('models/saved/svu_model.pth')
        
        # 评估模型
        logger.info("开始评估模型...")
        predictions = predictor.predict(test_data)
        mse = np.mean((predictions - test_data['labels'])**2)
        logger.info(f"测试集MSE: {mse:.4f}")
        
        logger.info("模型训练完成！")
        
    except Exception as e:
        logger.error(f"训练过程中出错: {str(e)}")
        raise

if __name__ == "__main__":
    main() 