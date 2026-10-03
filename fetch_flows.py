import pandas as pd
import requests
import akshare as ak
import yfinance as yf
from datetime import datetime
import json
import os

def fetch_a_share_sectors():
    """获取 A 股行业资金流向排名 (带节假日容错)"""
    print("正在抓取 A股 数据...")
    try:
        df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
        top_inflows = df.head(5)[['名称', '今日净流入-净额']].to_dict(orient='records')
        top_outflows = df.tail(5)[['名称', '今日净流入-净额']].to_dict(orient='records')
        return {"status": "success", "top_inflows": top_inflows, "top_outflows": top_outflows}
    except Exception as e:
        print(f"A股数据抓取异常 (可能因节假日休市): {e}")
        return {"status": "error", "message": "节假日休市或接口异常，无今日数据"}

def fetch_crypto_flows():
    """获取加密市场法币净流入估算"""
    print("正在抓取 加密市场 数据...")
    try:
        url = "https://stablecoins.llama.fi/stablecoincharts/all"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        recent_data = data[-2:]
        yesterday_cap = recent_data[0]['totalCirculatingUSD']['peggedUSD']
        today_cap = recent_data[1]['totalCirculatingUSD']['peggedUSD']
        daily_flow = today_cap - yesterday_cap
        
        return {
            "status": "success",
            "today_market_cap_usd": today_cap,
            "daily_net_flow_usd": daily_flow
        }
    except Exception as e:
        print(f"加密市场数据抓取异常: {e}")
        return {"status": "error", "message": str(e)}

def fetch_macro_etf_proxies():
    """获取全球宏观及美股资金流向替代指标"""
    print("正在抓取 宏观 ETF 数据...")
    tickers = ["SPY", "QQQ", "TLT", "GLD", "USO"]
    try:
        data = yf.download(tickers, period="2d", group_by="ticker", progress=False)
        results = []
        for ticker in tickers:
            if ticker in data:
                ticker_data = data[ticker]
                last_close = float(ticker_data['Close'].iloc[-1])
                last_vol = float(ticker_data['Volume'].iloc[-1])
                daily_turnover = last_close * last_vol
                
                results.append({
                    "ticker": ticker,
                    "close_price": round(last_close, 2),
                    "estimated_turnover_usd": round(daily_turnover, 2)
                })
        return {"status": "success", "data": results}
    except Exception as e:
        print(f"宏观 ETF 数据抓取异常: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    print(f"开始执行抓取任务 (UTC): {current_time}")
    
    # 组装最终的数据字典
    final_data = {
        "update_time_utc": current_time,
        "layer_2_ashare": fetch_a_share_sectors(),
        "layer_1_crypto": fetch_crypto_flows(),
        "layer_0_macro": fetch_macro_etf_proxies()
    }
    
    # 写入 JSON 文件
    file_name = "fund_flow_data.json"
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
        
    print(f"\n数据已成功保存至当前目录的 {file_name} 文件中。")
