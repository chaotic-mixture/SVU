# SVU (Standard Value Unit) 标准价值单位系统

SVU是一个不可流通的标准价值单位系统，用于衡量各种可计价物之间的相对价值。该系统通过图神经网络分析历史数据，建立稳定的价值锚点。

## 项目结构

```
SVU/
├── api/                    # API接口模块
│   ├── imf.py             # IMF数据接口
│   ├── bis.py             # BIS数据接口
│   ├── fred.py            # FRED数据接口
│   └── base.py            # 基础API类
├── core/                   # 核心功能模块
│   ├── svu_calculator.py  # SVU计算器
│   ├── data_processor.py  # 数据处理器
│   └── graph_builder.py   # 图构建器
├── data/                   # 数据存储
│   ├── raw/               # 原始数据
│   ├── processed/         # 处理后的数据
│   └── database/          # 数据库文件
├── models/                 # 模型定义
│   ├── gnn/               # 图神经网络
│   ├── lsm/               # 液态神经网络
│   └── database.py        # 数据库模型
├── utils/                  # 工具函数
│   ├── data_cleaner.py    # 数据清洗
│   ├── time_utils.py      # 时间处理
│   └── validators.py      # 数据验证
├── visualization/          # 可视化模块
│   ├── plot_3d.py         # 3D可视化
│   ├── plot_time.py       # 时间序列可视化
│   └── dashboard.py       # 交互式仪表板
├── scripts/               # 脚本文件
│   ├── train.py          # 训练脚本
│   ├── update_data.py    # 数据更新脚本
│   └── evaluate.py       # 评估脚本
├── tests/                 # 测试文件
├── config/                # 配置文件
│   ├── api_config.yaml   # API配置
│   └── model_config.yaml # 模型配置
├── main.py               # 主程序入口
├── requirements.txt      # 项目依赖
└── README.md            # 项目说明
```

## 功能特点

1. **数据采集**
   - 支持多个官方数据源（IMF、BIS、FRED等）
   - 自动数据更新和同步
   - 数据频率统一化

2. **数据处理**
   - 自动数据清洗和标准化
   - 时间序列对齐
   - 异常值检测和处理

3. **价值计算**
   - SVU基准值计算
   - 相对价值转换
   - 历史数据回溯

4. **图神经网络**
   - 动态图结构
   - 多维度特征学习
   - 实时更新支持

5. **可视化**
   - 3D交互式图表
   - 时间序列分析
   - 实时数据监控

## 安装说明

1. 克隆仓库：
```bash
git clone https://github.com/yourusername/SVU.git
cd SVU
```

2. 创建虚拟环境：
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. 安装依赖：
```bash
pip install -r requirements.txt
```

4. 配置API密钥：
```bash
cp config/api_config.yaml.example config/api_config.yaml
# 编辑api_config.yaml，添加您的API密钥
```

## 使用方法

1. 初始化数据库：
```bash
python scripts/init_db.py
```

2. 获取历史数据：
```bash
python scripts/update_data.py --start-date 2000-01-01
```

3. 训练模型：
```bash
python scripts/train.py --epochs 100
```

4. 启动可视化：
```bash
python scripts/visualize.py
```

## 数据源

- IMF (International Monetary Fund)
- BIS (Bank for International Settlements)
- FRED (Federal Reserve Economic Data)
- World Bank
- LBMA (London Bullion Market Association)

## 贡献指南

1. Fork 项目
2. 创建特性分支
3. 提交更改
4. 推送到分支
5. 创建 Pull Request

## 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件
