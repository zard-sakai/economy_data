import pandas as pd
import requests
import akshare as ak
import yfinance as yf
from datetime import datetime
import json
import warnings

# 忽略 yfinance 的一些常规警告
warnings.filterwarnings('ignore')

def fetch_ashare_sectors_30d():
    """获取 A 股核心板块过去 30 天的主力资金历史净流入"""
    print("正在抓取 A股 30天历史数据...")
    # 为了避免请求过多，MVP阶段我们硬编码选取几个最具代表性的核心板块
    core_sectors = ["半导体", "酿酒行业", "银行", "医疗器械"]
    sector_data = {}
    
    for sector in core_sectors:
        try:
            # 获取该板块的历史资金流向
            df = ak.stock_sector_fund_flow_hist(symbol=sector)
            # 取最近 30 个交易日
            df_30d = df.tail(30)
            
            daily_flows = []
            for _, row in df_30d.iterrows():
                daily_flows.append({
                    "date": str(row['日期']),
                    "close_price": float(row['收盘价']),
                    "change_pct": float(row['涨跌幅']),
                    # 东方财富的数据单位通常是元，我们转换为亿元方便前端展示
                    "net_flow_cny_100m": round(float(row['主力净流入-净额']) / 1e8, 2) 
                })
            sector_data[sector] = daily_flows
        except Exception as e:
            print(f"抓取A股 {sector} 失败: {e}")
            
    return {"status": "success", "data": sector_data}

def fetch_crypto_flows_30d():
    """获取加密市场过去 30 天的每日法币净流入"""
    print("正在抓取 加密市场 30天历史数据...")
    try:
        url = "https://stablecoins.llama.fi/stablecoincharts/all"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        # 取过去 31 天的数据（因为要算前一天的差值，所以多取一天）
        history_31d = data[-31:]
        
        daily_flows = []
        for i in range(1, len(history_31d)):
            today = history_31d[i]
            yesterday = history_31d[i-1]
            
            today_cap = float(today['totalCirculatingUSD']['peggedUSD'])
            yesterday_cap = float(yesterday['totalCirculatingUSD']['peggedUSD'])
            net_flow = today_cap - yesterday_cap
            
            # 时间戳转日期字符串
            date_str = datetime.utcfromtimestamp(int(today['date'])).strftime('%Y-%m-%d')
            
            daily_flows.append({
                "date": date_str,
                "market_cap_usd": round(today_cap, 2),
                "net_flow_usd": round(net_flow, 2)
            })
            
        return {"status": "success", "data": daily_flows}
    except Exception as e:
        print(f"加密市场数据抓取异常: {e}")
        return {"status": "error", "message": str(e)}

def fetch_macro_etf_30d():
    """获取全球宏观及美股 ETF 过去 30 天的伪资金流数据"""
    print("正在抓取 宏观 ETF 30天历史数据...")
    tickers = ["SPY", "QQQ", "TLT", "GLD", "USO"]
    try:
        # 下载过去 1 个月的数据（大约 22 个交易日）
        data = yf.download(tickers, period="1mo", group_by="ticker", progress=False)
        
        etf_data = {}
        for ticker in tickers:
            if ticker in data:
                ticker_data = data[ticker].dropna() # 清理空数据
                
                daily_flows = []
                for date, row in ticker_data.iterrows():
                    last_close = float(row['Close'])
                    last_open = float(row['Open'])
                    last_vol = float(row['Volume'])
                    
                    daily_turnover = last_close * last_vol
                    
                    # 伪资金流向算法：涨跌幅 * 放大系数 * 总成交额
                    price_change_pct = (last_close - last_open) / last_open
                    flow_multiplier = max(min(price_change_pct * 10, 1.0), -1.0)
                    estimated_net_flow = daily_turnover * flow_multiplier
                    
                    daily_flows.append({
                        "date": date.strftime('%Y-%m-%d'),
                        "close_price": round(last_close, 2),
                        "estimated_net_flow_usd": round(estimated_net_flow, 2),
                        "daily_turnover_usd": round(daily_turnover, 2)
                    })
                etf_data[ticker] = daily_flows
                
        return {"status": "success", "data": etf_data}
    except Exception as e:
        print(f"宏观 ETF 数据抓取异常: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    print(f"开始执行 30天历史数据 抓取任务 (UTC): {current_time}")
    
    final_data = {
        "update_time_utc": current_time,
        "layer_0_macro_30d": fetch_macro_etf_30d(),
        "layer_1_crypto_30d": fetch_crypto_flows_30d(),
        "layer_2_ashare_30d": fetch_ashare_sectors_30d()
    }
    
    file_name = "fund_flow_data.json"
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
        
    print(f"\n30天历史数据已成功保存至 {file_name} 文件中。")
