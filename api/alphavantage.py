from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime, timedelta
import requests
import logging
from .base import BaseAPI

logger = logging.getLogger(__name__)

class AlphaVantageAPI(BaseAPI):
    """Alpha Vantage API适配器类"""
    
    def __init__(self, config_path: str = 'config/api_config.yaml'):
        """初始化Alpha Vantage API
        
        Args:
            config_path: API配置文件路径
        """
        super().__init__(config_path)
        self.base_url = "https://www.alphavantage.co/query"
        self.api_key = self.config.get('alpha_vantage', {}).get('api_key')
        if not self.api_key:
            logger.warning("未配置Alpha Vantage API密钥")
        
    def get_historical_data(self,
                          start_date: str,
                          end_date: str,
                          frequency: str = 'daily') -> pd.DataFrame:
        """获取历史价格数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            frequency: 数据频率 (daily, weekly, monthly)
            
        Returns:
            pd.DataFrame: 历史价格数据
        """
        try:
            # 获取所有可用的资产列表
            assets = self._get_available_assets()
            
            # 获取每个资产的历史数据
            all_data = []
            for asset in assets:
                try:
                    data = self._get_asset_data(
                        asset['symbol'],
                        asset['type'],
                        start_date,
                        end_date,
                        frequency
                    )
                    if not data.empty:
                        all_data.append(data)
                except Exception as e:
                    logger.error(f"获取{asset['symbol']}数据时出错: {str(e)}")
            
            if not all_data:
                raise ValueError("没有获取到任何数据")
            
            # 合并所有数据
            df = pd.concat(all_data, ignore_index=True)
            
            # 清洗和验证数据
            df = self.clean_data(df)
            if not self.validate_data(df, ['date', 'symbol', 'price']):
                raise ValueError("数据验证失败")
            
            # 重采样到指定频率
            df = self.resample_data(df, frequency)
            
            return df
            
        except Exception as e:
            logger.error(f"获取历史数据失败: {str(e)}")
            return pd.DataFrame()
            
    def get_latest_data(self) -> pd.DataFrame:
        """获取最新价格数据
        
        Returns:
            pd.DataFrame: 最新价格数据
        """
        try:
            # 获取所有可用的资产列表
            assets = self._get_available_assets()
            
            # 获取每个资产的最新数据
            all_data = []
            for asset in assets:
                try:
                    data = self._get_latest_price(
                        asset['symbol'],
                        asset['type']
                    )
                    if not data.empty:
                        all_data.append(data)
                except Exception as e:
                    logger.error(f"获取{asset['symbol']}最新数据时出错: {str(e)}")
            
            if not all_data:
                raise ValueError("没有获取到任何数据")
            
            # 合并所有数据
            df = pd.concat(all_data, ignore_index=True)
            
            # 清洗和验证数据
            df = self.clean_data(df)
            if not self.validate_data(df, ['date', 'symbol', 'price']):
                raise ValueError("数据验证失败")
            
            return df
            
        except Exception as e:
            logger.error(f"获取最新数据失败: {str(e)}")
            return pd.DataFrame()
            
    def _get_available_assets(self) -> List[Dict]:
        """获取可用的资产列表
        
        Returns:
            List[Dict]: 资产列表，每个元素包含symbol和type
        """
        # 预定义的资产列表
        return [
            {'symbol': 'EUR/USD', 'type': 'forex'},
            {'symbol': 'GBP/USD', 'type': 'forex'},
            {'symbol': 'USD/JPY', 'type': 'forex'},
            {'symbol': 'GOLD', 'type': 'commodity'},
            {'symbol': 'SILVER', 'type': 'commodity'},
            {'symbol': 'OIL', 'type': 'commodity'}
        ]
            
    def _get_asset_data(self,
                       symbol: str,
                       asset_type: str,
                       start_date: str,
                       end_date: str,
                       frequency: str) -> pd.DataFrame:
        """获取特定资产的历史数据
        
        Args:
            symbol: 资产代码
            asset_type: 资产类型 (forex, commodity)
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            
        Returns:
            pd.DataFrame: 资产历史数据
        """
        try:
            # 构建请求参数
            params = {
                'apikey': self.api_key,
                'outputsize': 'full'
            }
            
            # 根据资产类型选择API端点
            if asset_type == 'forex':
                params['function'] = 'FX_DAILY'
                params['from_symbol'] = symbol.split('/')[0]
                params['to_symbol'] = symbol.split('/')[1]
            elif asset_type == 'commodity':
                params['function'] = 'TIME_SERIES_DAILY'
                params['symbol'] = symbol
            
            # 发送请求
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            # 解析响应数据
            data = response.json()
            
            # 转换为DataFrame
            if asset_type == 'forex':
                time_series = data.get('Time Series FX (Daily)', {})
            else:
                time_series = data.get('Time Series (Daily)', {})
            
            records = []
            for date, values in time_series.items():
                if start_date <= date <= end_date:
                    records.append({
                        'date': pd.to_datetime(date),
                        'symbol': symbol,
                        'price': float(values['4. close']),
                        'volume': float(values.get('5. volume', 0))
                    })
            
            df = pd.DataFrame(records)
            
            return df
            
        except Exception as e:
            logger.error(f"获取{symbol}历史数据失败: {str(e)}")
            return pd.DataFrame()
            
    def _get_latest_price(self,
                         symbol: str,
                         asset_type: str) -> pd.DataFrame:
        """获取特定资产的最新价格
        
        Args:
            symbol: 资产代码
            asset_type: 资产类型 (forex, commodity)
            
        Returns:
            pd.DataFrame: 最新价格数据
        """
        try:
            # 构建请求参数
            params = {
                'apikey': self.api_key
            }
            
            # 根据资产类型选择API端点
            if asset_type == 'forex':
                params['function'] = 'CURRENCY_EXCHANGE_RATE'
                params['from_currency'] = symbol.split('/')[0]
                params['to_currency'] = symbol.split('/')[1]
            elif asset_type == 'commodity':
                params['function'] = 'GLOBAL_QUOTE'
                params['symbol'] = symbol
            
            # 发送请求
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            # 解析响应数据
            data = response.json()
            
            # 转换为DataFrame
            if asset_type == 'forex':
                price = float(data['Realtime Currency Exchange Rate']['5. Exchange Rate'])
            else:
                price = float(data['Global Quote']['05. price'])
            
            df = pd.DataFrame([{
                'date': datetime.now(),
                'symbol': symbol,
                'price': price
            }])
            
            return df
            
        except Exception as e:
            logger.error(f"获取{symbol}最新价格失败: {str(e)}")
            return pd.DataFrame() 