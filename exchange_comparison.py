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
        
        # 连接状态
        self.backpack_connected = False
        self.zklighter_connected = False
        
        # 停止事件
        self.stop_event = asyncio.Event()
    
    async def close(self):
        """关闭所有资源"""
        pass
    
    def handle_backpack_ticker(self, symbol, ticker_data):
        """处理Backpack价格数据"""
        # 初始化数据结构（如果需要）
        if symbol not in self.backpack_data:
            self.backpack_data[symbol] = {}
        
        # 从ticker数据中提取我们需要的字段
        if 'last' in ticker_data:  # 'last' 是最新价格
            try:
                self.backpack_data[symbol]['last_price'] = float(ticker_data['last'])
            except (ValueError, TypeError):
                logger.warning(f"无法转换Backpack价格: {ticker_data['last']}")
        
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
        """获取Backpack资金费率数据"""
        try:
            logger.info("正在获取Backpack资金费率数据...")
            
            # 创建需要获取资金费率的交易对列表
            pairs_to_fetch = []
            for pair in BACKPACK_TRADING_PAIRS:
                if pair.endswith("_PERP"):  # 只获取永续合约
                    pairs_to_fetch.append(pair)
            
            if not pairs_to_fetch:
                logger.warning("没有需要获取资金费率的交易对")
                return
            
            async with httpx.AsyncClient() as client:
                for symbol in pairs_to_fetch:
                    try:
                        # 使用正确的资金费率API路径
                        url = f"{BACKPACK_REST_URL}/api/v1/fundingRates?symbol={symbol}"
                        logger.debug(f"请求资金费率: {url}")
                        
                        response = await client.get(url)
                        if response.status_code != 200:
                            logger.error(f"获取{symbol}资金费率失败: HTTP {response.status_code}")
                            continue
                            
                        funding_data = response.json()
                        
                        # 确保返回的数据是列表且不为空
                        if not funding_data or not isinstance(funding_data, list):
                            logger.warning(f"{symbol}资金费率数据格式错误: {funding_data}")
                            continue
                        
                        # 获取最新的资金费率（列表中的第一个元素）
                        latest_funding = funding_data[0]
                        if "fundingRate" not in latest_funding:
                            logger.warning(f"{symbol}资金费率数据缺少fundingRate字段: {latest_funding}")
                            continue
                        
                        # 确保交易对有初始化的数据结构
                        if symbol not in self.backpack_data:
                            self.backpack_data[symbol] = {}
                            
                        # 解析资金费率并转换为百分比
                        funding_rate = float(latest_funding["fundingRate"]) * 100
                        self.backpack_data[symbol]['funding_rate'] = funding_rate
                        
                        # 记录当前时间作为更新时间
                        self.backpack_data[symbol]['funding_time'] = time.time()
                        
                        logger.info(f"Backpack {symbol} 资金费率: {funding_rate:+.6f}%")
                        
                        # 避免API请求过于频繁
                        await asyncio.sleep(0.2)
                    
                    except Exception as e:
                        logger.error(f"获取{symbol}资金费率时出错: {e}")
                
                # 更新计数器和时间
                self.backpack_last_update = time.time()
                
                # 更新统一数据
                self.update_unified_data()
                
                logger.info("Backpack资金费率数据更新完成")
        except Exception as e:
            logger.error(f"获取Backpack资金费率时出错: {e}")
            logger.error(traceback.format_exc())
    
    async def fetch_backpack_prices(self):
        """获取Backpack所有交易对的价格"""
        try:
            # 使用REST API获取所有价格
            async with httpx.AsyncClient() as client:
                url = f"{BACKPACK_REST_URL}/api/v1/tickers"
                logger.debug(f"请求所有ticker数据: {url}")
                
                response = await client.get(url)
                
                if response.status_code == 200:
                    tickers = response.json()
                    
                    for ticker in tickers:
                        symbol = ticker.get("symbol")
                        
                        # 只处理我们关注的交易对
                        if symbol in BACKPACK_TRADING_PAIRS:
                            # 初始化数据结构（如果需要）
                            if symbol not in self.backpack_data:
                                self.backpack_data[symbol] = {}
                            
                            # 转换价格数据
                            try:
                                self.backpack_data[symbol]['last_price'] = float(ticker.get('lastPrice', 0))
                            except (ValueError, TypeError):
                                logger.warning(f"无法转换Backpack价格: {ticker.get('lastPrice')}")
                            
                            # 记录更新时间
                            self.backpack_data[symbol]['update_time'] = time.time()
                    
                    # 更新统一格式数据
                    self.update_unified_data()
                    
                    logger.info(f"已获取Backpack价格数据")
                else:
                    logger.warning(f"获取Backpack价格数据失败，状态码: {response.status_code}")
        except Exception as e:
            logger.error(f"获取Backpack价格数据出错: {e}")
    
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

async def main():
    """主函数"""
    # 创建数据收集器
    collector = ExchangeDataCollector()
    
    # 获取初始数据
    print("正在获取初始数据...")
    await collector.fetch_backpack_prices()
    await collector.fetch_backpack_funding_rates()
    
    # 创建任务
    tasks = [
        asyncio.create_task(connect_zklighter(collector)),
        asyncio.create_task(connect_backpack(collector)),
        asyncio.create_task(update_display(collector)),
        asyncio.create_task(update_funding_rates(collector))
    ]
    
    try:
        # 等待用户中断
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        # 取消所有任务
        for task in tasks:
            task.cancel()
        # 等待任务取消完成
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        print("\n程序已终止")
    finally:
        # 关闭收集器资源
        await collector.close()

async def connect_zklighter(collector):
    """连接到zkLighter WebSocket并获取数据"""
    while True:
        try:
            collector.zklighter_connected = False
            logger.info("正在连接到zkLighter WebSocket API...")
            
            async with websockets.connect(ZKLIGHTER_WS_URL) as ws:
                collector.zklighter_connected = True
                logger.info("已连接到zkLighter WebSocket API")
                
                # 订阅市场数据
                for market_id in ZKLIGHTER_MARKET_IDS:
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": f"market_stats/{market_id}"
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.debug(f"已订阅zkLighter市场ID={market_id}")
                
                # 保持连接的ping计时器
                last_ping_time = time.time()
                
                # 不断接收消息
                while True:
                    try:
                        # 每30秒发送ping保持连接活跃
                        current_time = time.time()
                        if current_time - last_ping_time > 30:
                            logger.debug("向zkLighter发送ping")
                            pong_waiter = await ws.ping()
                            try:
                                await asyncio.wait_for(pong_waiter, timeout=5)
                                logger.debug("收到zkLighter的pong响应")
                            except asyncio.TimeoutError:
                                logger.warning("zkLighter pong响应超时，重新连接")
                                break
                            last_ping_time = current_time
                        
                        # 设置接收超时，以便定期发送ping
                        message = await asyncio.wait_for(ws.recv(), timeout=10)
                        
                        # 解析并处理消息
                        try:
                            data = json.loads(message)
                            if data.get('type') in ['update/market_stats', 'subscribed/market_stats'] and 'market_stats' in data:
                                collector.handle_zklighter_market_stats(data['market_stats'])
                        except json.JSONDecodeError:
                            logger.warning(f"无法解析zkLighter消息: {message[:100]}...")
                    
                    except asyncio.TimeoutError:
                        # 接收超时，继续循环（可能会触发ping）
                        continue
                    except Exception as e:
                        logger.error(f"处理zkLighter消息时出错: {e}")
                        if isinstance(e, websockets.exceptions.ConnectionClosed):
                            logger.warning("zkLighter连接已关闭，重新连接")
                            break
        
        except Exception as e:
            collector.zklighter_connected = False
            logger.error(f"zkLighter WebSocket连接出错: {e}")
        
        # 等待5秒后重试
        logger.info("5秒后重新连接到zkLighter...")
        await asyncio.sleep(5)

async def connect_backpack(collector):
    """连接到Backpack WebSocket并获取数据"""
    while True:
        try:
            collector.backpack_connected = False
            logger.info("正在连接到Backpack WebSocket API...")
            
            async with websockets.connect(BACKPACK_WS_URL) as ws:
                collector.backpack_connected = True
                logger.info("已连接到Backpack WebSocket API")
                
                # 订阅所有交易对
                for trading_pair in BACKPACK_TRADING_PAIRS:
                    subscribe_msg = {
                        "op": "subscribe",
                        "channel": "ticker",
                        "market": trading_pair
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.debug(f"已订阅Backpack交易对: {trading_pair}")
                
                # 保持连接的ping计时器
                last_ping_time = time.time()
                
                # 不断接收消息
                while True:
                    try:
                        # 每30秒发送ping保持连接活跃
                        current_time = time.time()
                        if current_time - last_ping_time > 30:
                            logger.debug("向Backpack发送ping")
                            pong_waiter = await ws.ping()
                            try:
                                await asyncio.wait_for(pong_waiter, timeout=5)
                                logger.debug("收到Backpack的pong响应")
                            except asyncio.TimeoutError:
                                logger.warning("Backpack pong响应超时，重新连接")
                                break
                            last_ping_time = current_time
                        
                        # 设置接收超时，以便定期发送ping
                        message = await asyncio.wait_for(ws.recv(), timeout=10)
                        
                        # 解析并处理消息
                        try:
                            data = json.loads(message)
                            
                            # 处理订阅确认
                            if "type" in data and data.get("type") == "subscribed":
                                logger.debug(f"已确认订阅Backpack: {data.get('channel')} - {data.get('market')}")
                                continue
                            
                            # 处理行情更新
                            if "type" in data and data.get("type") == "update" and data.get("channel") == "ticker":
                                market = data.get("market")
                                ticker_data = data.get("data", {})
                                if market and ticker_data:
                                    collector.handle_backpack_ticker(market, ticker_data)
                        
                        except json.JSONDecodeError:
                            logger.warning(f"无法解析Backpack消息: {message[:100]}...")
                    
                    except asyncio.TimeoutError:
                        # 接收超时，继续循环（可能会触发ping）
                        continue
                    except Exception as e:
                        logger.error(f"处理Backpack消息时出错: {e}")
                        if isinstance(e, websockets.exceptions.ConnectionClosed):
                            logger.warning("Backpack连接已关闭，重新连接")
                            break
        
        except Exception as e:
            collector.backpack_connected = False
            logger.error(f"Backpack WebSocket连接出错: {e}")
        
        # 等待5秒后重试
        logger.info("5秒后重新连接到Backpack...")
        await asyncio.sleep(5)

async def update_display(collector):
    """定期更新表格显示"""
    update_interval = 3  # 每3秒更新一次
    
    while True:
        try:
            # 显示表格
            collector.display_comparison_table()
            
            # 等待下次更新
            await asyncio.sleep(update_interval)
        except Exception as e:
            logger.error(f"更新表格显示出错: {e}")
            await asyncio.sleep(update_interval)

async def update_funding_rates(collector):
    """定期更新资金费率"""
    update_interval = 60  # 每60秒更新一次
    
    while True:
        try:
            # 更新资金费率
            await collector.fetch_backpack_funding_rates()
            
            # 等待下次更新
            await asyncio.sleep(update_interval)
        except Exception as e:
            logger.error(f"更新资金费率出错: {e}")
            await asyncio.sleep(5)  # 出错时等待5秒

if __name__ == "__main__":
    try:
        # 运行主程序
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序已终止")
    except Exception as e:
        logger.error(f"主程序异常: {e}")
        logger.error(traceback.format_exc()) 