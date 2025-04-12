#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import websockets
import time
import traceback
import ssl
import os
import sys
import logging
import httpx

# 导入PrettyTable库（如果可用）
try:
    from prettytable import PrettyTable
    HAS_PRETTYTABLE = True
except ImportError:
    print("警告: 未安装prettytable库，将使用简单格式显示数据")
    HAS_PRETTYTABLE = False

# 交易所WebSocket API地址
ZKLIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
BACKPACK_WS_URL = "wss://ws.backpack.exchange"
BACKPACK_REST_URL = "https://api.backpack.exchange"

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("exchange_comparison")

# 交易对和币种映射
CURRENCY_MAPPING = {
    # Backpack交易对
    "BTC_USDC_PERP": "BTC",
    "ETH_USDC_PERP": "ETH",
    "SOL_USDC_PERP": "SOL",
    "AVAX_USDC_PERP": "AVAX",
    "AAVE_USDC_PERP": "AAVE",
    "ADA_USDC_PERP": "ADA",
    "ARB_USDC_PERP": "ARB",
    "BERA_USDC_PERP": "BERA",
    "BNB_USDC_PERP": "BNB",
    "DOGE_USDC_PERP": "DOGE",
    "DOT_USDC_PERP": "DOT",
    "ENA_USDC_PERP": "ENA",
    "FARTCOIN_USDC_PERP": "FARTCOIN",
    "HYPE_USDC_PERP": "HYPE",
    "IP_USDC_PERP": "IP",
    "JUP_USDC_PERP": "JUP",
    "KAITO_USDC_PERP": "KAITO",
    "LINK_USDC_PERP": "LINK",
    "LTC_USDC_PERP": "LTC",
    "ONDO_USDC_PERP": "ONDO",
    "SUI_USDC_PERP": "SUI",
    "S_USDC_PERP": "S",
    "TRUMP_USDC_PERP": "TRUMP",
    "WIF_USDC_PERP": "WIF",
    "XRP_USDC_PERP": "XRP",
    
    # zkLighter市场ID
    0: "ETH",
    1: "BTC",
    2: "SOL",
    3: "AVAX",
    4: "AAVE",
    5: "ADA",
    6: "ARB",
    7: "BERA",
    8: "BNB",
    9: "DOGE",
    10: "DOT",
    11: "ENA",
    12: "FARTCOIN",
    13: "HYPE",
    14: "IP",
    15: "JUP",
    16: "KAITO",
    17: "LINK",
    18: "LTC",
    19: "ONDO",
    20: "SUI",
    21: "S",
    22: "TRUMP",
    23: "WIF",
    24: "XRP",
}

# Backpack交易对列表
BACKPACK_TRADING_PAIRS = [
    "BTC_USDC_PERP",
    "ETH_USDC_PERP",
    "SOL_USDC_PERP",
    "AVAX_USDC_PERP",
    "AAVE_USDC_PERP",
    "ADA_USDC_PERP",
    "ARB_USDC_PERP",
    "BERA_USDC_PERP",
    "BNB_USDC_PERP",
    "DOGE_USDC_PERP",
    "DOT_USDC_PERP",
    "ENA_USDC_PERP",
    "FARTCOIN_USDC_PERP",
    "HYPE_USDC_PERP",
    "IP_USDC_PERP",
    "JUP_USDC_PERP",
    "KAITO_USDC_PERP",
    "LINK_USDC_PERP",
    "LTC_USDC_PERP",
    "ONDO_USDC_PERP",
    "SUI_USDC_PERP",
    "S_USDC_PERP",
    "TRUMP_USDC_PERP",
    "WIF_USDC_PERP",
    "XRP_USDC_PERP",
]

# zkLighter市场ID列表
ZKLIGHTER_MARKET_IDS = list(range(25))  # [0, 1, 2, ..., 24]

class ExchangeDataCollector:
    """收集和显示所有交易所数据的类"""
    
    def __init__(self, backpack_api_key=None, backpack_api_secret=None):
        # 初始化数据结构
        self.backpack_data = {}  # Backpack交易所的数据
        self.zklighter_data = {}  # zkLighter交易所的数据
        self.unified_data = {}  # 统一格式的数据，用于对比显示
        
        # 更新计数和时间
        self.backpack_update_count = 0
        self.zklighter_update_count = 0
        self.backpack_last_update = 0
        self.zklighter_last_update = 0
        
        # API密钥
        self.backpack_api_key = backpack_api_key
        self.backpack_api_secret = backpack_api_secret
        
        # HTTP客户端
        self.http_client = httpx.AsyncClient(timeout=10.0)
    
    async def close(self):
        """关闭HTTP客户端"""
        await self.http_client.aclose()
    
    def handle_backpack_ticker(self, symbol, ticker_data):
        """处理Backpack价格数据"""
        if symbol not in self.backpack_data:
            self.backpack_data[symbol] = {}
        
        # 从ticker数据中提取我们需要的字段
        if 'c' in ticker_data:  # 'c' 是最新价格
            try:
                self.backpack_data[symbol]['last_price'] = float(ticker_data['c'])
            except (ValueError, TypeError):
                logger.warning(f"无法转换Backpack价格: {ticker_data['c']}")
        
        # 记录更新时间
        self.backpack_data[symbol]['update_time'] = time.time()
        
        # 更新计数和时间
        self.backpack_update_count += 1
        self.backpack_last_update = time.time()
        
        # 更新统一格式数据
        self.update_unified_data()
    
    def handle_zklighter_market_stats(self, market_stats):
        """处理zkLighter市场统计数据"""
        if not market_stats or 'market_id' not in market_stats:
            return
        
        market_id = market_stats['market_id']
        
        # 更新市场数据
        if market_id not in self.zklighter_data:
            self.zklighter_data[market_id] = {}
        
        # 提取关心的字段
        for field in ['market_id', 'index_price', 'mark_price', 'last_trade_price', 'current_funding_rate']:
            if field in market_stats:
                # 对数值字段进行类型转换
                if field in ['index_price', 'mark_price', 'last_trade_price', 'current_funding_rate']:
                    try:
                        self.zklighter_data[market_id][field] = float(market_stats[field])
                    except (ValueError, TypeError):
                        logger.warning(f"无法转换zkLighter字段 {field}: {market_stats[field]}")
                else:
                    self.zklighter_data[market_id][field] = market_stats[field]
        
        # 记录更新时间
        self.zklighter_data[market_id]['update_time'] = time.time()
        
        # 更新计数和时间
        self.zklighter_update_count += 1
        self.zklighter_last_update = time.time()
        
        # 更新统一格式数据
        self.update_unified_data()
    
    async def fetch_backpack_funding_rates(self):
        """获取Backpack所有交易对的资金费率"""
        try:
            # 先尝试单独获取每个交易对的资金费率
            for symbol in BACKPACK_TRADING_PAIRS:
                try:
                    # 构建API请求URL
                    url = f"{BACKPACK_REST_URL}/api/v1/fundingRates?symbol={symbol}"
                    logger.debug(f"请求资金费率: {url}")
                    
                    # 发送请求
                    response = await self.http_client.get(url)
                    
                    if response.status_code == 200:
                        # 解析响应
                        data = response.json()
                        
                        # 处理数据
                        if data and isinstance(data, list) and len(data) > 0:
                            for item in data:
                                if "fundingRate" in item:
                                    try:
                                        # 将资金费率乘以100，与backpack_market_table.py保持一致
                                        funding_rate = float(item["fundingRate"]) * 100
                                        
                                        # 更新市场数据
                                        if symbol not in self.backpack_data:
                                            self.backpack_data[symbol] = {}
                                        
                                        self.backpack_data[symbol]['funding_rate'] = funding_rate
                                        
                                        logger.debug(f"获取到Backpack {symbol}资金费率: {funding_rate}")
                                        break  # 找到了就跳出循环
                                    except (ValueError, TypeError) as e:
                                        logger.warning(f"无法转换{symbol}的资金费率: {item.get('fundingRate')}, 错误: {e}")
                    else:
                        logger.warning(f"获取{symbol}资金费率失败，状态码: {response.status_code}")
                except Exception as e:
                    logger.error(f"获取{symbol}资金费率出错: {e}")
            
            # 更新统一格式数据
            self.update_unified_data()
            
            logger.info(f"已获取Backpack资金费率数据")
        except Exception as e:
            logger.error(f"获取Backpack资金费率出错: {e}")
            logger.debug(traceback.format_exc())
    
    async def fetch_backpack_prices(self):
        """获取Backpack所有交易对的价格"""
        try:
            # 使用REST API获取所有价格
            url = f"{BACKPACK_REST_URL}/api/v1/tickers"
            logger.debug(f"请求所有ticker数据: {url}")
            
            response = await self.http_client.get(url)
            
            if response.status_code == 200:
                tickers = response.json()
                
                for ticker in tickers:
                    symbol = ticker.get("symbol")
                    
                    # 只处理我们关注的交易对
                    if symbol in BACKPACK_TRADING_PAIRS:
                        # 更新市场数据
                        if symbol not in self.backpack_data:
                            self.backpack_data[symbol] = {}
                        
                        # 转换价格数据
                        for field_name, api_field in [
                            ('last_price', 'lastPrice'),
                            ('index_price', 'indexPrice'),
                            ('mark_price', 'markPrice')
                        ]:
                            field_value = ticker.get(api_field, ticker.get('lastPrice') if api_field != 'lastPrice' else None)
                            if field_value is not None:
                                try:
                                    self.backpack_data[symbol][field_name] = float(field_value)
                                except (ValueError, TypeError):
                                    logger.warning(f"无法转换Backpack {api_field}: {field_value}")
                        
                        # 记录更新时间
                        self.backpack_data[symbol]['update_time'] = time.time()
                
                # 更新统一格式数据
                self.update_unified_data()
                
                logger.info(f"已获取Backpack价格数据")
            else:
                logger.warning(f"获取Backpack价格数据失败，状态码: {response.status_code}")
        except Exception as e:
            logger.error(f"获取Backpack价格数据出错: {e}")
            logger.debug(traceback.format_exc())
    
    def update_unified_data(self):
        """更新统一格式的数据，用于对比显示"""
        # 清空旧数据
        self.unified_data = {}
        
        # 处理zkLighter数据
        for market_id, data in self.zklighter_data.items():
            if market_id in CURRENCY_MAPPING:
                symbol = CURRENCY_MAPPING[market_id]
                
                if symbol not in self.unified_data:
                    self.unified_data[symbol] = {"zklighter": {}, "backpack": {}}
                
                # 复制数据
                for src_field, dst_field in [
                    ('last_trade_price', 'last_price'),
                    ('index_price', 'index_price'),
                    ('mark_price', 'mark_price'),
                    ('current_funding_rate', 'funding_rate')
                ]:
                    if src_field in data:
                        self.unified_data[symbol]["zklighter"][dst_field] = data[src_field]
        
        # 处理Backpack数据
        for trading_pair, data in self.backpack_data.items():
            if trading_pair in CURRENCY_MAPPING:
                symbol = CURRENCY_MAPPING[trading_pair]
                
                if symbol not in self.unified_data:
                    self.unified_data[symbol] = {"zklighter": {}, "backpack": {}}
                
                # 复制数据
                for field in ['last_price', 'index_price', 'mark_price', 'funding_rate']:
                    if field in data:
                        self.unified_data[symbol]["backpack"][field] = data[field]
        
        # 计算资金费率差异
        for symbol, data in self.unified_data.items():
            zklighter_funding = data["zklighter"].get("funding_rate")
            backpack_funding = data["backpack"].get("funding_rate")
            
            if zklighter_funding is not None and backpack_funding is not None:
                # 确保两者都是浮点数
                try:
                    zk_funding_float = float(zklighter_funding)
                    bp_funding_float = float(backpack_funding)
                    
                    # 由于zkLighter是1小时费率，Backpack是8小时费率，需要乘以8来做公平对比
                    zk_funding_float = zk_funding_float * 8
                    
                    # 计算差值: zkLighter - Backpack
                    funding_diff = zk_funding_float - bp_funding_float
                    data["funding_diff"] = funding_diff
                except (ValueError, TypeError) as e:
                    logger.warning(f"计算{symbol}资金费率差异时出错: {e}")
    
    def display_comparison_table(self):
        """显示交易所对比表格"""
        if not self.unified_data:
            print("\n暂无市场数据，等待数据接收...")
            return
        
        try:
            # 清屏（Windows和类Unix系统）
            os.system('cls' if os.name == 'nt' else 'clear')
            
            # 定义护眼绿色文本颜色
            GREEN_TEXT = "\033[92m"  # 亮绿色
            RESET_COLOR = "\033[0m"  # 重置颜色
            
            print(GREEN_TEXT)  # 开始使用护眼绿色
            print("\n" + "=" * 100)
            print(f"交易所资金费率对比 (更新时间: {time.strftime('%Y-%m-%d %H:%M:%S')})")
            print("=" * 100)
            
            if HAS_PRETTYTABLE:
                # 创建表格
                table = PrettyTable()
                table.field_names = [
                    "币种", 
                    "zkLighter价格", "Backpack价格", 
                    "zkLighter资金费率", "Backpack资金费率", 
                    "资金费率差值", "套利机会"
                ]
                
                # 设置表格样式
                table.align = "r"  # 右对齐
                table.align["币种"] = "l"  # 币种名称左对齐
                table.align["套利机会"] = "c"  # 套利机会居中
                
                # 按照资金费率差值的绝对值从大到小排序
                sorted_data = []
                for symbol, data in self.unified_data.items():
                    funding_diff = data.get('funding_diff')
                    # 只有当差值存在时才添加到排序列表
                    if funding_diff is not None:
                        sorted_data.append((symbol, abs(funding_diff)))
                
                # 按差值绝对值从大到小排序
                sorted_data.sort(key=lambda x: x[1], reverse=True)
                sorted_symbols = [item[0] for item in sorted_data]
                
                # 添加没有资金费率差值的币种到列表末尾
                for symbol in self.unified_data.keys():
                    if symbol not in sorted_symbols:
                        sorted_symbols.append(symbol)
                
                # 添加数据行
                for symbol in sorted_symbols:
                    try:
                        data = self.unified_data[symbol]
                        
                        # 格式化数据
                        # 价格数据
                        zk_price = f"{float(data['zklighter'].get('last_price', 0)):.2f}" if 'last_price' in data['zklighter'] else "N/A"
                        bp_price = f"{float(data['backpack'].get('last_price', 0)):.2f}" if 'last_price' in data['backpack'] else "N/A"
                        
                        # 资金费率（以百分比格式显示）
                        zk_funding = data['zklighter'].get('funding_rate')
                        bp_funding = data['backpack'].get('funding_rate')
                        
                        if zk_funding is not None:
                            # zkLighter资金费率需要乘以8（因为它是1小时费率，而Backpack是8小时费率）
                            zk_funding = float(zk_funding) * 8
                            zk_funding_str = f"{zk_funding:+.4f}%" if zk_funding != 0 else "0.0000%"
                        else:
                            zk_funding_str = "N/A"
                        
                        if bp_funding is not None:
                            bp_funding = float(bp_funding)
                            bp_funding_str = f"{bp_funding:+.4f}%" if bp_funding != 0 else "0.0000%"
                        else:
                            bp_funding_str = "N/A"
                        
                        # 资金费率差值
                        funding_diff = data.get('funding_diff')
                        if funding_diff is not None:
                            funding_diff = float(funding_diff)
                            funding_diff_str = f"{funding_diff:+.4f}%" if funding_diff != 0 else "0.0000%"
                            
                            # 套利机会判断 (绝对差值大于0.05%视为有套利机会)
                            arbitrage = "✓" if abs(funding_diff) >= 0.05 else ""
                        else:
                            funding_diff_str = "N/A"
                            arbitrage = ""
                        
                        # 添加到表格
                        table.add_row([
                            symbol,
                            zk_price,
                            bp_price,
                            zk_funding_str,
                            bp_funding_str,
                            funding_diff_str,
                            arbitrage
                        ])
                    except Exception as e:
                        logger.error(f"显示{symbol}数据时出错: {e}")
                        continue
                
                # 打印表格
                print(table)
            else:
                # 没有PrettyTable时使用简单格式显示
                print("币种     | zkLighter价格 | Backpack价格 | zkLighter资金费率 | Backpack资金费率 | 资金费率差值 | 套利")
                print("-" * 90)
                
                # 按照资金费率差值的绝对值从大到小排序
                sorted_data = []
                for symbol, data in self.unified_data.items():
                    funding_diff = data.get('funding_diff')
                    # 只有当差值存在时才添加到排序列表
                    if funding_diff is not None:
                        sorted_data.append((symbol, abs(funding_diff)))
                
                # 按差值绝对值从大到小排序
                sorted_data.sort(key=lambda x: x[1], reverse=True)
                sorted_symbols = [item[0] for item in sorted_data]
                
                # 添加没有资金费率差值的币种到列表末尾
                for symbol in self.unified_data.keys():
                    if symbol not in sorted_symbols:
                        sorted_symbols.append(symbol)
                
                for symbol in sorted_symbols:
                    try:
                        data = self.unified_data[symbol]
                        
                        zk_price = f"{float(data['zklighter'].get('last_price', 0)):.2f}" if 'last_price' in data['zklighter'] else "N/A"
                        bp_price = f"{float(data['backpack'].get('last_price', 0)):.2f}" if 'last_price' in data['backpack'] else "N/A"
                        
                        zk_funding = data['zklighter'].get('funding_rate')
                        bp_funding = data['backpack'].get('funding_rate')
                        
                        if zk_funding is not None:
                            # zkLighter资金费率需要乘以8
                            zk_funding = float(zk_funding) * 8
                            zk_funding_str = f"{zk_funding:+.4f}%" if zk_funding != 0 else "0.0000%"
                        else:
                            zk_funding_str = "N/A"
                        
                        if bp_funding is not None:
                            bp_funding = float(bp_funding)
                            bp_funding_str = f"{bp_funding:+.4f}%" if bp_funding != 0 else "0.0000%"
                        else:
                            bp_funding_str = "N/A"
                        
                        funding_diff = data.get('funding_diff')
                        if funding_diff is not None:
                            funding_diff = float(funding_diff)
                            funding_diff_str = f"{funding_diff:+.4f}%" if funding_diff != 0 else "0.0000%"
                            arbitrage = "✓" if abs(funding_diff) >= 0.05 else ""
                        else:
                            funding_diff_str = "N/A"
                            arbitrage = ""
                        
                        print(f"{symbol:10} | {zk_price:12} | {bp_price:12} | {zk_funding_str:17} | {bp_funding_str:17} | {funding_diff_str:14} | {arbitrage:5}")
                    except Exception as e:
                        logger.error(f"显示{symbol}数据时出错: {e}")
                        continue
            
            print(RESET_COLOR)  # 重置颜色到默认
            update_info = []
            if self.zklighter_last_update > 0:
                zk_ago = time.time() - self.zklighter_last_update
                update_info.append(f"zkLighter更新: {time.strftime('%H:%M:%S', time.localtime(self.zklighter_last_update))} ({zk_ago:.1f}秒前)")
            
            if self.backpack_last_update > 0:
                bp_ago = time.time() - self.backpack_last_update
                update_info.append(f"Backpack更新: {time.strftime('%H:%M:%S', time.localtime(self.backpack_last_update))} ({bp_ago:.1f}秒前)")
            
            print(" | ".join(update_info))
            print(f"共显示 {len(self.unified_data)} 个币种 | zkLighter更新: {self.zklighter_update_count}次, Backpack更新: {self.backpack_update_count}次")
            print("=" * 100)
            print("按 Ctrl+C 退出程序")
        except Exception as e:
            logger.error(f"显示表格时出错: {e}")
            logger.debug(traceback.format_exc())

async def monitor_exchanges(backpack_api_key=None, backpack_api_secret=None):
    """监控多个交易所数据"""
    print("交易所数据对比工具")
    print("=" * 50)
    
    # 创建数据收集器
    collector = ExchangeDataCollector(backpack_api_key, backpack_api_secret)
    
    try:
        # 创建SSL上下文
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # 先获取一次初始数据
        print("正在获取初始数据...")
        await collector.fetch_backpack_prices()
        await collector.fetch_backpack_funding_rates()
        
        # 创建多个异步任务
        zklighter_task = asyncio.create_task(connect_zklighter(collector, ssl_context))
        backpack_task = asyncio.create_task(connect_backpack(collector, ssl_context))
        display_task = asyncio.create_task(update_display(collector))
        funding_update_task = asyncio.create_task(update_funding_rates(collector))
        
        # 等待所有任务完成（实际上会一直运行直到被中断）
        await asyncio.gather(
            zklighter_task, 
            backpack_task, 
            display_task, 
            funding_update_task
        )
    
    except asyncio.CancelledError:
        logger.info("任务被取消")
    except Exception as e:
        logger.error(f"监控交易所数据时出错: {e}")
        logger.debug(traceback.format_exc())
    
    finally:
        # 确保关闭HTTP客户端
        await collector.close()

async def connect_zklighter(collector, ssl_context):
    """连接到zkLighter WebSocket并处理数据"""
    while True:
        try:
            logger.info("正在连接到zkLighter交易所WebSocket API...")
            
            # 创建WebSocket连接
            async with websockets.connect(
                ZKLIGHTER_WS_URL, 
                ssl=ssl_context,
                ping_interval=None,
                close_timeout=10,
                max_size=10 * 1024 * 1024
            ) as websocket:
                logger.info("zkLighter WebSocket连接已建立")
                
                # 为每个市场ID创建订阅
                for market_id in ZKLIGHTER_MARKET_IDS:
                    subscribe_message = {
                        "type": "subscribe",
                        "channel": f"market_stats/{market_id}"
                    }
                    await websocket.send(json.dumps(subscribe_message))
                    logger.debug(f"已订阅zkLighter市场ID={market_id}")
                
                # 持续接收和处理消息
                while True:
                    try:
                        response = await websocket.recv()
                        
                        # 解析响应
                        try:
                            data = json.loads(response)
                        except json.JSONDecodeError:
                            continue
                        
                        # 处理市场统计数据
                        if data.get('type') in ['update/market_stats', 'subscribed/market_stats'] and 'market_stats' in data:
                            collector.handle_zklighter_market_stats(data['market_stats'])
                        
                    except Exception as e:
                        logger.error(f"处理zkLighter消息时出错: {e}")
                        if not isinstance(e, asyncio.TimeoutError):
                            logger.debug(traceback.format_exc())
        
        except websockets.exceptions.ConnectionClosedError as e:
            logger.error(f"zkLighter WebSocket连接已关闭: {e}")
            logger.info("尝试重新连接zkLighter...")
            await asyncio.sleep(5)
            continue
        
        except Exception as e:
            logger.error(f"zkLighter连接错误: {e}")
            logger.debug(traceback.format_exc())
            await asyncio.sleep(5)
            continue

async def connect_backpack(collector, ssl_context):
    """连接到Backpack WebSocket并处理数据"""
    while True:
        try:
            logger.info("正在连接到Backpack交易所WebSocket API...")
            
            # 创建WebSocket连接
            async with websockets.connect(
                BACKPACK_WS_URL, 
                ssl=ssl_context,
                ping_interval=None,
                close_timeout=10,
                max_size=10 * 1024 * 1024
            ) as websocket:
                logger.info("Backpack WebSocket连接已建立")
                
                # 订阅所有交易对的ticker数据
                subscription_channels = [f"ticker.{symbol}" for symbol in BACKPACK_TRADING_PAIRS]
                
                # 构建订阅消息
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": subscription_channels,
                    "id": 1
                }
                
                # 发送订阅请求
                await websocket.send(json.dumps(subscribe_msg))
                logger.info(f"已发送Backpack订阅请求，共 {len(subscription_channels)} 个交易对")
                
                # 持续接收和处理消息
                while True:
                    try:
                        response = await websocket.recv()
                        
                        # 解析响应
                        try:
                            data = json.loads(response)
                        except json.JSONDecodeError:
                            logger.warning(f"无法解析Backpack WebSocket响应: {response[:100]}...")
                            continue
                        
                        # 处理订阅确认消息
                        if "id" in data and data.get("id") == 1 and "result" in data:
                            logger.info("Backpack订阅确认: " + str(data.get("result")))
                            continue
                        
                        # 处理ticker数据
                        if "data" in data and "stream" in data:
                            stream = data["stream"]
                            if stream.startswith("ticker."):
                                symbol = stream[7:]  # 去掉"ticker."前缀
                                ticker_data = data["data"]
                                collector.handle_backpack_ticker(symbol, ticker_data)
                        
                    except Exception as e:
                        logger.error(f"处理Backpack消息时出错: {e}")
                        if not isinstance(e, asyncio.TimeoutError):
                            logger.debug(traceback.format_exc())
        
        except websockets.exceptions.ConnectionClosedError as e:
            logger.error(f"Backpack WebSocket连接已关闭: {e}")
            logger.info("尝试重新连接Backpack...")
            await asyncio.sleep(5)
            continue
        
        except Exception as e:
            logger.error(f"Backpack连接错误: {e}")
            logger.debug(traceback.format_exc())
            await asyncio.sleep(5)
            continue

async def update_display(collector):
    """定期更新显示"""
    update_interval = 3  # 秒
    
    while True:
        try:
            # 显示表格
            collector.display_comparison_table()
            logger.info("表格已更新")
            
            # 等待到下次更新
            await asyncio.sleep(update_interval)
        
        except Exception as e:
            logger.error(f"更新显示时出错: {e}")
            logger.debug(traceback.format_exc())
            await asyncio.sleep(update_interval)

async def update_funding_rates(collector):
    """定期更新资金费率"""
    update_interval = 60  # 秒
    
    while True:
        try:
            # 更新Backpack资金费率
            await collector.fetch_backpack_funding_rates()
            
            # 等待到下次更新
            await asyncio.sleep(update_interval)
        
        except Exception as e:
            logger.error(f"更新资金费率时出错: {e}")
            logger.debug(traceback.format_exc())
            await asyncio.sleep(update_interval)

if __name__ == "__main__":
    try:
        # 可选: 从环境变量获取API密钥
        backpack_api_key = os.environ.get("BACKPACK_API_KEY", "")
        backpack_api_secret = os.environ.get("BACKPACK_API_SECRET", "")
        
        # 启动监控
        asyncio.run(monitor_exchanges(backpack_api_key, backpack_api_secret))
    except KeyboardInterrupt:
        print("\n程序已终止")
    except Exception as e:
        logger.error(f"主程序异常: {e}")
        logger.debug(traceback.format_exc()) 