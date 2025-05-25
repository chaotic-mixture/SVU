from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime, timedelta
import requests
import logging
from .base import BaseAPI

logger = logging.getLogger(__name__)

class CoinGeckoAPI(BaseAPI):
    """CoinGecko API适配器类"""
    
    def __init__(self, config_path: str = 'config/api_config.yaml'):
        """初始化CoinGecko API
        
        Args:
            config_path: API配置文件路径
        """
        super().__init__(config_path)
        self.base_url = "https://api.coingecko.com/api/v3"
        self.rate_limit = 50  # 每分钟请求限制
        
    def get_historical_data(self,
                          start_date: str,
                          end_date: str,
                          frequency: str = 'daily') -> pd.DataFrame:
        """获取历史价格数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            frequency: 数据频率 (daily, hourly)
            
        Returns:
            pd.DataFrame: 历史价格数据
        """
        try:
            # 获取所有可用的加密货币列表
            coins = self._get_available_coins()
            
            # 获取每个币种的历史数据
            all_data = []
            for coin in coins:
                try:
                    data = self._get_coin_data(
                        coin['id'],
                        start_date,
                        end_date,
                        frequency
                    )
                    if not data.empty:
                        all_data.append(data)
                except Exception as e:
                    logger.error(f"获取{coin['id']}数据时出错: {str(e)}")
            
            if not all_data:
                raise ValueError("没有获取到任何数据")
            
            # 合并所有数据
            df = pd.concat(all_data, ignore_index=True)
            
            # 清洗和验证数据
            df = self.clean_data(df)
            if not self.validate_data(df, ['date', 'coin_id', 'price']):
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
            # 获取所有可用的加密货币列表
            coins = self._get_available_coins()
            
            # 构建请求URL
            url = f"{self.base_url}/simple/price"
            params = {
                'ids': ','.join([coin['id'] for coin in coins]),
                'vs_currencies': 'usd',
                'include_market_cap': 'true',
                'include_24hr_vol': 'true',
                'include_24hr_change': 'true'
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            # 解析响应数据
            data = response.json()
            
            # 转换为DataFrame
            records = []
            for coin_id, prices in data.items():
                records.append({
                    'date': datetime.now(),
                    'coin_id': coin_id,
                    'price': prices['usd'],
                    'market_cap': prices.get('usd_market_cap'),
                    'volume_24h': prices.get('usd_24h_vol'),
                    'change_24h': prices.get('usd_24h_change')
                })
            
            df = pd.DataFrame(records)
            
            # 清洗和验证数据
            df = self.clean_data(df)
            if not self.validate_data(df, ['date', 'coin_id', 'price']):
                raise ValueError("数据验证失败")
            
            return df
            
        except Exception as e:
            logger.error(f"获取最新数据失败: {str(e)}")
            return pd.DataFrame()
            
    def _get_available_coins(self) -> List[Dict]:
        """获取可用的加密货币列表
        
        Returns:
            List[Dict]: 加密货币列表，每个元素包含id和symbol
        """
        try:
            url = f"{self.base_url}/coins/list"
            response = self.session.get(url)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"获取可用加密货币列表失败: {str(e)}")
            return []
            
    def _get_coin_data(self,
                      coin_id: str,
                      start_date: str,
                      end_date: str,
                      frequency: str) -> pd.DataFrame:
        """获取特定加密货币的历史数据
        
        Args:
            coin_id: 加密货币ID
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            
        Returns:
            pd.DataFrame: 加密货币历史数据
        """
        try:
            # 构建请求URL
            url = f"{self.base_url}/coins/{coin_id}/market_chart/range"
            params = {
                'vs_currency': 'usd',
                'from': int(datetime.strptime(start_date, '%Y-%m-%d').timestamp()),
                'to': int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            # 解析响应数据
            data = response.json()
            
            # 转换为DataFrame
            records = []
            for timestamp, price in data['prices']:
                records.append({
                    'date': datetime.fromtimestamp(timestamp / 1000),
                    'coin_id': coin_id,
                    'price': price
                })
            
            df = pd.DataFrame(records)
            
            return df
            
        except Exception as e:
            logger.error(f"获取{coin_id}历史数据失败: {str(e)}")
            return pd.DataFrame() 