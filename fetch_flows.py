import pandas as pd
import requests
import akshare as ak
import yfinance as yf
from datetime import datetime

def fetch_a_share_sectors():
    """获取 A 股行业资金流向排名 (Layer 2)"""
    print("\n--- [1] A股行业资金流向 (数据源: 东方财富 via AkShare) ---")
    try:
        # 获取今日行业资金流向排名
        df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
        # 提取前 5 名吸金板块和前 5 名失血板块
        top_inflows = df.head(5)[['名称', '今日净流入-净额', '今日净流入-净占比']]
        top_outflows = df.tail(5)[['名称', '今日净流入-净额', '今日净流入-净占比']]
        
        print("【净流入 Top 5】")
        print(top_inflows.to_string(index=False))
        print("\n【净流出 Top 5】")
        print(top_outflows.to_string(index=False))
    except Exception as e:
        print(f"A股数据抓取失败: {e}")

def fetch_crypto_flows():
    """获取加密市场法币净流入估算 (Layer 1)"""
    print("\n--- [2] 加密市场法币流入/稳定币市值 (数据源: DefiLlama) ---")
    try:
        url = "https://stablecoins.llama.fi/stablecoincharts/all"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        # 获取最近两天的总市值数据
        recent_data = data[-2:]
        yesterday_cap = recent_data[0]['totalCirculatingUSD']['peggedUSD']
        today_cap = recent_data[1]['totalCirculatingUSD']['peggedUSD']
        daily_flow = today_cap - yesterday_cap
        
        print(f"今日全网稳定币总市值: ${today_cap / 1e9:.2f} Billion")
        print(f"昨日全网稳定币总市值: ${yesterday_cap / 1e9:.2f} Billion")
        print(f"24小时法币净流入估算: ${daily_flow / 1e6:.2f} Million")
    except Exception as e:
        print(f"加密市场数据抓取失败: {e}")

def fetch_macro_etf_proxies():
    """获取全球宏观及美股资金流向替代指标 (Layer 0)"""
    print("\n--- [3] 全球宏观资产 ETF 交易数据 (数据源: Yahoo Finance) ---")
    # SPY(标普), QQQ(纳指), TLT(美长债), GLD(黄金), USO(原油)
    tickers = ["SPY", "QQQ", "TLT", "GLD", "USO"]
    try:
        # 获取最近两天的交易数据
        data = yf.download(tickers, period="2d", group_by="ticker", progress=False)
        
        results = []
        for ticker in tickers:
            if ticker in data:
                ticker_data = data[ticker]
                # 计算替代指标：单日成交额估算 = 收盘价 * 成交量
                last_close = ticker_data['Close'].iloc[-1]
                last_vol = ticker_data['Volume'].iloc[-1]
                daily_turnover = last_close * last_vol
                
                results.append({
                    "资产/ETF": ticker,
                    "最新收盘价": round(float(last_close), 2),
                    "单日成交额估算($)": f"${daily_turnover / 1e6:.2f}M"
                })
        
        df_results = pd.DataFrame(results)
        print(df_results.to_string(index=False))
        print("注：美股免费接口无直接资金净额，此处展示成交额活跃度作为替代。")
    except Exception as e:
        print(f"宏观 ETF 数据抓取失败: {e}")

if __name__ == "__main__":
    print(f"执行时间 (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    fetch_a_share_sectors()
    fetch_crypto_flows()
    fetch_macro_etf_proxies()
