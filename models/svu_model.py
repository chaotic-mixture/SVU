import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, global_mean_pool
import numpy as np
from typing import List, Dict, Tuple, Optional

class SVUGraphModel(nn.Module):
    """SVU图神经网络模型"""
    
    def __init__(self,
                 input_dim: int = 5,
                 hidden_dim: int = 64,
                 output_dim: int = 1,
                 num_layers: int = 3):
        """初始化模型
        
        Args:
            input_dim: 输入特征维度
            hidden_dim: 隐藏层维度
            output_dim: 输出维度
            num_layers: 图卷积层数量
        """
        super(SVUGraphModel, self).__init__()
        
        # 图卷积层
        self.convs = nn.ModuleList()
        self.convs.append(GCNConv(input_dim, hidden_dim))
        for _ in range(num_layers - 1):
            self.convs.append(GCNConv(hidden_dim, hidden_dim))
            
        # 全连接层
        self.fc1 = nn.Linear(hidden_dim, hidden_dim // 2)
        self.fc2 = nn.Linear(hidden_dim // 2, output_dim)
        
        # Dropout层
        self.dropout = nn.Dropout(0.2)
        
    def forward(self,
                x: torch.Tensor,
                edge_index: torch.Tensor,
                batch: Optional[torch.Tensor] = None) -> torch.Tensor:
        """前向传播
        
        Args:
            x: 节点特征矩阵
            edge_index: 边索引矩阵
            batch: 批处理索引
            
        Returns:
            torch.Tensor: 预测结果
        """
        # 图卷积层
        for conv in self.convs:
            x = conv(x, edge_index)
            x = F.relu(x)
            x = self.dropout(x)
            
        # 全局池化
        if batch is not None:
            x = global_mean_pool(x, batch)
            
        # 全连接层
        x = F.relu(self.fc1(x))
        x = self.dropout(x)
        x = self.fc2(x)
        
        return x

class SVUPredictor:
    """SVU预测器"""
    
    def __init__(self,
                 model: SVUGraphModel,
                 device: str = 'cuda' if torch.cuda.is_available() else 'cpu'):
        """初始化预测器
        
        Args:
            model: 图神经网络模型
            device: 计算设备
        """
        self.model = model.to(device)
        self.device = device
        
    def prepare_graph_data(self,
                          features: np.ndarray,
                          edge_index: np.ndarray) -> Tuple[torch.Tensor, torch.Tensor]:
        """准备图数据
        
        Args:
            features: 节点特征
            edge_index: 边索引
            
        Returns:
            Tuple[torch.Tensor, torch.Tensor]: 处理后的特征和边索引
        """
        x = torch.FloatTensor(features).to(self.device)
        edge_index = torch.LongTensor(edge_index).to(self.device)
        return x, edge_index
        
    def train(self,
              train_data: Dict[str, np.ndarray],
              epochs: int = 100,
              lr: float = 0.001) -> List[float]:
        """训练模型
        
        Args:
            train_data: 训练数据
            epochs: 训练轮数
            lr: 学习率
            
        Returns:
            List[float]: 训练损失历史
        """
        self.model.train()
        optimizer = torch.optim.Adam(self.model.parameters(), lr=lr)
        criterion = nn.MSELoss()
        
        x, edge_index = self.prepare_graph_data(
            train_data['features'],
            train_data['edge_index']
        )
        y = torch.FloatTensor(train_data['labels']).to(self.device)
        
        losses = []
        for epoch in range(epochs):
            optimizer.zero_grad()
            out = self.model(x, edge_index)
            loss = criterion(out, y)
            loss.backward()
            optimizer.step()
            
            losses.append(loss.item())
            if (epoch + 1) % 10 == 0:
                print(f'Epoch {epoch+1}/{epochs}, Loss: {loss.item():.4f}')
                
        return losses
        
    def predict(self,
                test_data: Dict[str, np.ndarray]) -> np.ndarray:
        """预测
        
        Args:
            test_data: 测试数据
            
        Returns:
            np.ndarray: 预测结果
        """
        self.model.eval()
        with torch.no_grad():
            x, edge_index = self.prepare_graph_data(
                test_data['features'],
                test_data['edge_index']
            )
            out = self.model(x, edge_index)
            return out.cpu().numpy()
            
    def save_model(self, path: str) -> None:
        """保存模型
        
        Args:
            path: 保存路径
        """
        torch.save(self.model.state_dict(), path)
        
    def load_model(self, path: str) -> None:
        """加载模型
        
        Args:
            path: 模型路径
        """
        self.model.load_state_dict(torch.load(path)) 