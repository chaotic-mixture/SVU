from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime, timedelta
import requests
import logging
from .base import BaseAPI

logger = logging.getLogger(__name__)

class WorldBankAPI(BaseAPI):
    """World Bank API适配器类"""
    
    def __init__(self, config_path: str = 'config/api_config.yaml'):
        """初始化World Bank API
        
        Args:
            config_path: API配置文件路径
        """
        super().__init__(config_path)
        self.base_url = "http://api.worldbank.org/v2"
        
    def get_historical_data(self,
                          start_date: str,
                          end_date: str,
                          frequency: str = 'yearly') -> pd.DataFrame:
        """获取历史经济指标数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            frequency: 数据频率 (yearly, quarterly)
            
        Returns:
            pd.DataFrame: 历史经济指标数据
        """
        try:
            # 获取所有可用的指标列表
            indicators = self._get_available_indicators()
            
            # 获取每个指标的历史数据
            all_data = []
            for indicator in indicators:
                try:
                    data = self._get_indicator_data(
                        indicator['id'],
                        start_date,
                        end_date,
                        frequency
                    )
                    if not data.empty:
                        all_data.append(data)
                except Exception as e:
                    logger.error(f"获取{indicator['id']}数据时出错: {str(e)}")
            
            if not all_data:
                raise ValueError("没有获取到任何数据")
            
            # 合并所有数据
            df = pd.concat(all_data, ignore_index=True)
            
            # 清洗和验证数据
            df = self.clean_data(df)
            if not self.validate_data(df, ['date', 'indicator_id', 'value']):
                raise ValueError("数据验证失败")
            
            # 重采样到指定频率
            df = self.resample_data(df, frequency)
            
            return df
            
        except Exception as e:
            logger.error(f"获取历史数据失败: {str(e)}")
            return pd.DataFrame()
            
    def get_latest_data(self) -> pd.DataFrame:
        """获取最新经济指标数据
        
        Returns:
            pd.DataFrame: 最新经济指标数据
        """
        try:
            # 获取所有可用的指标列表
            indicators = self._get_available_indicators()
            
            # 获取每个指标的最新数据
            all_data = []
            for indicator in indicators:
                try:
                    data = self._get_latest_value(indicator['id'])
                    if not data.empty:
                        all_data.append(data)
                except Exception as e:
                    logger.error(f"获取{indicator['id']}最新数据时出错: {str(e)}")
            
            if not all_data:
                raise ValueError("没有获取到任何数据")
            
            # 合并所有数据
            df = pd.concat(all_data, ignore_index=True)
            
            # 清洗和验证数据
            df = self.clean_data(df)
            if not self.validate_data(df, ['date', 'indicator_id', 'value']):
                raise ValueError("数据验证失败")
            
            return df
            
        except Exception as e:
            logger.error(f"获取最新数据失败: {str(e)}")
            return pd.DataFrame()
            
    def _get_available_indicators(self) -> List[Dict]:
        """获取可用的经济指标列表
        
        Returns:
            List[Dict]: 经济指标列表，每个元素包含id和name
        """
        # 预定义的经济指标列表
        return [
            {'id': 'NY.GDP.MKTP.CD', 'name': 'GDP (current US$)'},
            {'id': 'NY.GDP.MKTP.KD', 'name': 'GDP (constant 2015 US$)'},
            {'id': 'NY.GDP.PCAP.CD', 'name': 'GDP per capita (current US$)'},
            {'id': 'NY.GDP.PCAP.KD', 'name': 'GDP per capita (constant 2015 US$)'},
            {'id': 'FP.CPI.TOTL', 'name': 'Consumer price index (2010 = 100)'},
            {'id': 'NE.TRD.GNFS.ZS', 'name': 'Trade (% of GDP)'}
        ]
            
    def _get_indicator_data(self,
                          indicator_id: str,
                          start_date: str,
                          end_date: str,
                          frequency: str) -> pd.DataFrame:
        """获取特定经济指标的历史数据
        
        Args:
            indicator_id: 指标ID
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            
        Returns:
            pd.DataFrame: 经济指标历史数据
        """
        try:
            # 构建请求URL
            url = f"{self.base_url}/country/all/indicator/{indicator_id}"
            params = {
                'format': 'json',
                'per_page': 1000,
                'date': f"{start_date[:4]}:{end_date[:4]}"
            }
            
            # 发送请求
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            # 解析响应数据
            data = response.json()
            
            # 转换为DataFrame
            records = []
            for item in data[1]:
                records.append({
                    'date': pd.to_datetime(f"{item['date']}-01-01"),
                    'indicator_id': indicator_id,
                    'value': float(item['value']) if item['value'] is not None else None,
                    'country': item['country']['value']
                })
            
            df = pd.DataFrame(records)
            
            return df
            
        except Exception as e:
            logger.error(f"获取{indicator_id}历史数据失败: {str(e)}")
            return pd.DataFrame()
            
    def _get_latest_value(self, indicator_id: str) -> pd.DataFrame:
        """获取特定经济指标的最新值
        
        Args:
            indicator_id: 指标ID
            
        Returns:
            pd.DataFrame: 最新经济指标数据
        """
        try:
            # 构建请求URL
            url = f"{self.base_url}/country/all/indicator/{indicator_id}"
            params = {
                'format': 'json',
                'per_page': 1,
                'sort': 'date:desc'
            }
            
            # 发送请求
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            # 解析响应数据
            data = response.json()
            
            # 转换为DataFrame
            if data[1]:
                item = data[1][0]
                df = pd.DataFrame([{
                    'date': pd.to_datetime(f"{item['date']}-01-01"),
                    'indicator_id': indicator_id,
                    'value': float(item['value']) if item['value'] is not None else None,
                    'country': item['country']['value']
                }])
            else:
                df = pd.DataFrame()
            
            return df
            
        except Exception as e:
            logger.error(f"获取{indicator_id}最新值失败: {str(e)}")
            return pd.DataFrame() 