from typing import Dict, List, Optional, Tuple
import pandas as pd
import networkx as nx
from datetime import datetime, timedelta
import logging
from sqlalchemy.orm import Session
from .database import Item, Price, ExchangeRate

logger = logging.getLogger(__name__)

class ValueGraph:
    """价值图结构类，用于构建和管理价值关系图"""
    
    def __init__(self, db_session: Session):
        """初始化价值图
        
        Args:
            db_session: 数据库会话
        """
        self.db = db_session
        self.graph = nx.DiGraph()
        self.timestamp = None
        
    def build_graph(self,
                   start_date: datetime,
                   end_date: datetime,
                   min_confidence: float = 0.7) -> None:
        """构建价值关系图
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            min_confidence: 最小置信度
        """
        try:
            # 清空现有图
            self.graph.clear()
            
            # 获取所有物品
            items = self.db.query(Item).all()
            
            # 添加节点
            for item in items:
                self.graph.add_node(
                    item.id,
                    name=item.name,
                    symbol=item.symbol,
                    type=item.type
                )
            
            # 获取价格数据
            prices = self.db.query(Price).filter(
                Price.timestamp.between(start_date, end_date),
                Price.confidence >= min_confidence
            ).all()
            
            # 获取汇率数据
            rates = self.db.query(ExchangeRate).filter(
                ExchangeRate.timestamp.between(start_date, end_date),
                ExchangeRate.confidence >= min_confidence
            ).all()
            
            # 添加价格边
            for price in prices:
                self.graph.add_edge(
                    'SVU',  # 假设SVU是基准节点
                    price.item_id,
                    weight=price.price,
                    timestamp=price.timestamp,
                    source=price.source,
                    confidence=price.confidence,
                    type='price'
                )
            
            # 添加汇率边
            for rate in rates:
                self.graph.add_edge(
                    rate.source_item_id,
                    rate.target_item_id,
                    weight=rate.rate,
                    timestamp=rate.timestamp,
                    source=rate.source,
                    confidence=rate.confidence,
                    type='exchange_rate'
                )
            
            # 计算节点属性
            self._compute_node_attributes()
            
            # 更新图的时间戳
            self.timestamp = end_date
            
            logger.info(f"成功构建价值图，包含{len(self.graph.nodes)}个节点和{len(self.graph.edges)}条边")
            
        except Exception as e:
            logger.error(f"构建价值图失败: {str(e)}")
            raise
            
    def _compute_node_attributes(self) -> None:
        """计算节点属性"""
        try:
            # 计算每个节点的波动性得分
            for node in self.graph.nodes:
                # 获取节点的所有入边和出边
                in_edges = list(self.graph.in_edges(node, data=True))
                out_edges = list(self.graph.out_edges(node, data=True))
                
                # 计算价格/汇率的波动性
                prices = [edge['weight'] for edge in in_edges + out_edges]
                if prices:
                    volatility = pd.Series(prices).std() / pd.Series(prices).mean()
                else:
                    volatility = 0.0
                
                # 更新节点属性
                self.graph.nodes[node]['volatility'] = volatility
                
                # 计算节点的中心性
                self.graph.nodes[node]['in_degree'] = len(in_edges)
                self.graph.nodes[node]['out_degree'] = len(out_edges)
                
        except Exception as e:
            logger.error(f"计算节点属性失败: {str(e)}")
            raise
            
    def get_node_attributes(self, node_id: int) -> Dict:
        """获取节点属性
        
        Args:
            node_id: 节点ID
            
        Returns:
            Dict: 节点属性
        """
        return self.graph.nodes[node_id]
        
    def get_edge_attributes(self, source_id: int, target_id: int) -> Dict:
        """获取边属性
        
        Args:
            source_id: 源节点ID
            target_id: 目标节点ID
            
        Returns:
            Dict: 边属性
        """
        return self.graph.edges[source_id, target_id]
        
    def get_shortest_path(self,
                         source_id: int,
                         target_id: int,
                         weight: str = 'confidence') -> List[int]:
        """获取最短路径
        
        Args:
            source_id: 源节点ID
            target_id: 目标节点ID
            weight: 权重属性
            
        Returns:
            List[int]: 路径节点ID列表
        """
        try:
            path = nx.shortest_path(
                self.graph,
                source=source_id,
                target=target_id,
                weight=weight
            )
            return path
        except nx.NetworkXNoPath:
            logger.warning(f"未找到从{source_id}到{target_id}的路径")
            return []
            
    def get_central_nodes(self, top_n: int = 10) -> List[Tuple[int, float]]:
        """获取中心节点
        
        Args:
            top_n: 返回的节点数量
            
        Returns:
            List[Tuple[int, float]]: 节点ID和中心性得分的元组列表
        """
        try:
            # 计算PageRank中心性
            centrality = nx.pagerank(self.graph)
            
            # 按中心性得分排序
            sorted_nodes = sorted(
                centrality.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            return sorted_nodes[:top_n]
            
        except Exception as e:
            logger.error(f"计算中心节点失败: {str(e)}")
            return []
            
    def get_volatile_nodes(self,
                          threshold: float = 0.1,
                          top_n: int = 10) -> List[Tuple[int, float]]:
        """获取波动性高的节点
        
        Args:
            threshold: 波动性阈值
            top_n: 返回的节点数量
            
        Returns:
            List[Tuple[int, float]]: 节点ID和波动性得分的元组列表
        """
        try:
            # 获取所有节点的波动性
            volatilities = [
                (node, self.graph.nodes[node]['volatility'])
                for node in self.graph.nodes
            ]
            
            # 过滤并排序
            volatile_nodes = [
                (node, vol) for node, vol in volatilities
                if vol >= threshold
            ]
            volatile_nodes.sort(key=lambda x: x[1], reverse=True)
            
            return volatile_nodes[:top_n]
            
        except Exception as e:
            logger.error(f"获取波动性节点失败: {str(e)}")
            return []
            
    def to_dataframe(self) -> pd.DataFrame:
        """将图转换为DataFrame
        
        Returns:
            pd.DataFrame: 图的边数据
        """
        try:
            # 获取所有边
            edges = []
            for source, target, data in self.graph.edges(data=True):
                edges.append({
                    'source_id': source,
                    'target_id': target,
                    'weight': data['weight'],
                    'timestamp': data['timestamp'],
                    'source': data['source'],
                    'confidence': data['confidence'],
                    'type': data['type']
                })
            
            return pd.DataFrame(edges)
            
        except Exception as e:
            logger.error(f"转换图为DataFrame失败: {str(e)}")
            return pd.DataFrame() 