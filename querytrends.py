from trendspy import Trends
import pandas as pd
import json
import time
import random
from datetime import datetime
import requests
from urllib.parse import quote
import re
import os
from dotenv import load_dotenv

load_dotenv()

def get_proxy():
    """获取代理，优先使用隧道代理，其次动态代理"""
    # 隧道代理
    tunnel = os.getenv('KDL_TUNNEL')
    tunnel_user = os.getenv('KDL_TUNNEL_USERNAME')
    tunnel_pwd = os.getenv('KDL_TUNNEL_PASSWORD')
    if tunnel and tunnel_user and tunnel_pwd:
        proxies = {
            "http": f"http://{tunnel_user}:{tunnel_pwd}@{tunnel}/",
            "https": f"http://{tunnel_user}:{tunnel_pwd}@{tunnel}/"
        }
        print(f"使用隧道代理: {tunnel}")
        return proxies

    # 动态代理
    api_url = os.getenv('KDL_API_URL')
    username = os.getenv('KDL_USERNAME')
    password = os.getenv('KDL_PASSWORD')
    if not api_url or not username or not password:
        return None
    try:
        proxy_ip = requests.get(api_url, timeout=10).text.strip()
        proxies = {
            "http": f"http://{username}:{password}@{proxy_ip}/",
            "https": f"http://{username}:{password}@{proxy_ip}/"
        }
        print(f"使用动态代理: {proxy_ip}")
        return proxies
    except Exception as e:
        print(f"获取代理失败: {e}")
        return None

def get_related_queries(keyword, geo='', timeframe='today 12-m', max_retries=5):
    """
    获取关键词的相关查询数据，带请求限制
    """
    for attempt in range(1, max_retries + 1):
        proxies = get_proxy()
        tr = Trends(hl='zh-CN', proxy=proxies, request_delay=3.0) if proxies else Trends(hl='zh-CN')
        # 禁用keep-alive和连接池，确保每次请求建立新连接（隧道代理每个新连接换IP）
        if proxies:
            tr.session.headers.update({'Connection': 'close'})
            from requests.adapters import HTTPAdapter
            tr.session.mount('http://', HTTPAdapter(pool_connections=0, pool_maxsize=0))
            tr.session.mount('https://', HTTPAdapter(pool_connections=0, pool_maxsize=0))
            print(f"[代理] {tr.session.proxies}")
        # 清除cookie，避免Google通过cookie追踪
        tr.session.cookies.clear()

        # 随机化 User-Agent
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

        headers = {
            'referer': 'https://www.google.com/',
            'User-Agent': random.choice(user_agents),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }

        try:
            # 检查请求限制
            request_limiter.wait_if_needed()

            # 添加随机延时
            delay = random.uniform(1, 3)
            time.sleep(delay)

            related_data = tr.related_queries(
                keyword,
                headers=headers,
                geo=geo,
                timeframe=timeframe
            )
            print(f"成功获取数据！")
            return related_data

        except Exception as e:
            error_msg = str(e)
            print(f"[{keyword}] 第{attempt}/{max_retries}次尝试失败: {error_msg}")

            if attempt < max_retries:
                wait_time = random.uniform(5, 15)
                print(f"换代理重试，等待 {wait_time:.1f} 秒...")
                time.sleep(wait_time)
                continue
            else:
                print(f"[{keyword}] 已达最大重试次数{max_retries}，跳过")
                return None

def batch_get_queries(keywords, geo='', timeframe='today 12-m', delay_between_queries=5):
    """
    批量获取多个关键词的数据，带间隔控制
    """
    results = {}
    
    for keyword in keywords:
        try:
            print(f"\n正在查询关键词: {keyword}")
            results[keyword] = get_related_queries(keyword, geo, timeframe)
            
            # 在请求之间添加延时
            if keyword != keywords[-1]:  # 如果不是最后一个关键词
                delay = delay_between_queries + random.uniform(0, 2)  # 基础延时加0-2秒的随机延时
                print(f"等待 {delay:.1f} 秒后继续下一个查询...")
                time.sleep(delay)
                
        except Exception as e:
            print(f"获取 {keyword} 的数据失败: {str(e)}")
            results[keyword] = None
            
            # 如果遇到错误，增加额外等待时间
            time.sleep(10)
    
    return results

def save_related_queries(keyword, related_data):
    """
    保存相关查询数据到JSON文件
    """
    if not related_data:
        return
    
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    json_data = {
        'keyword': keyword,
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'related_queries': {
            'top': related_data['top'].to_dict(orient='records') if isinstance(related_data.get('top'), pd.DataFrame) else related_data.get('top'),
            'rising': related_data['rising'].to_dict(orient='records') if isinstance(related_data.get('rising'), pd.DataFrame) else related_data.get('rising')
        }
    }
    
    # 保存为JSON文件
    filename = f"related_queries_{keyword}_{timestamp}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    
    return filename

def print_related_queries(related_data):
    """
    打印相关查询词数据
    """
    if not related_data:
        print("没有相关查询数据")
        return
    
    print("\n相关查询词统计:")
    print("=" * 50)
    
    # 打印热门查询
    if 'top' in related_data and related_data['top'] is not None:
        print("\n热门查询:")
        print("-" * 30)
        df = related_data['top']
        if isinstance(df, pd.DataFrame):
            for _, row in df.iterrows():
                print(f"- {row['query']:<30} (相关度: {row['value']})")
    
    # 打印上升趋势查询
    if 'rising' in related_data and related_data['rising'] is not None:
        print("\n上升趋势查询:")
        print("-" * 30)
        df = related_data['rising']
        if isinstance(df, pd.DataFrame):
            for _, row in df.iterrows():
                print(f"- {row['query']:<30} (增长: {row['value']})")


# 主函数
# timeframe可能的值：
# today 12-m：12个月
# now 1-d：1天
# now 7-d：7天
# now 30-d：30天
# now 90-d：90天
# 日期格式：2024-12-28 2024-12-30
def main():
    # 设置要查询的关键词列表
    keywords = ['game']  # 可以添加多个关键词
    geo = ''
    timeframe = 'now 1-d'
    
    print("开始批量查询...")
    print(f"地区: {geo if geo else '全球'}")
    print(f"时间范围: {timeframe}")
    
    try:
        # 批量获取数据
        results = batch_get_queries(
            keywords,
            geo=geo,
            timeframe=timeframe,
            delay_between_queries=100  # 设置请求间隔
        )

        # 处理和保存结果
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        for keyword, data in results.items():
            if data:
                print(f"\n处理 {keyword} 的数据:")
                print_related_queries(data)
                filename = save_related_queries(keyword, data)
                print(f"数据已保存到文件: {filename}")
            else:
                print(f"\n未能获取 {keyword} 的数据")
                
    except Exception as e:
        print(f"批量查询过程中出错: {str(e)}")

class RequestLimiter:
    def __init__(self):
        self.requests = []  # 存储请求时间戳
        self.max_requests_per_min = 30  # 每分钟最大请求数
        self.max_requests_per_hour = 200  # 每小时最大请求数
        
    def can_make_request(self):
        """检查是否可以发起新请求"""
        current_time = time.time()
        
        # 清理超过1小时的旧请求记录
        self.requests = [t for t in self.requests if current_time - t < 3600]
        
        # 获取最近1分钟的请求数
        recent_min_requests = len([t for t in self.requests if current_time - t < 60])
        
        # 获取最近1小时的请求数
        recent_hour_requests = len(self.requests)
        
        if (recent_min_requests >= self.max_requests_per_min or 
            recent_hour_requests >= self.max_requests_per_hour):
            return False
        
        return True
    
    def add_request(self):
        """记录新的请求"""
        self.requests.append(time.time())
    
    def wait_if_needed(self):
        """如果需要，等待直到可以发送请求"""
        while not self.can_make_request():
            wait_time = random.uniform(5, 10)
            print(f"达到请求限制，等待 {wait_time:.1f} 秒...")
            time.sleep(wait_time)
        self.add_request()

# 创建全局请求限制器
request_limiter = RequestLimiter()

if __name__ == "__main__":
    main()
