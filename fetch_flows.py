import pandas as pd
import requests
import akshare as ak
import yfinance as yf
from datetime import datetime
import json
import time
import warnings

warnings.filterwarnings('ignore')

def safe_yf_download(tickers, period="10d"):
    """安全下载 yfinance 数据，带重试机制并禁用多线程以防止 SQLite 锁库"""
    for attempt in range(3):
        try:
            data = yf.download(
                tickers, 
                period=period, 
                group_by="ticker", 
                progress=False, 
                threads=False, 
                ignore_tz=True
            )
            if not data.empty:
                return data
        except Exception as e:
            time.sleep(2)
    return pd.DataFrame()


# ----------------------------------------------------------------------
# 看板 1：大类资产流动 (Layer 0 - Macro Assets)
# ----------------------------------------------------------------------
def fetch_layer0_macro_7d():
    print("正在抓取 [看板1: 大类资产] 最近 7 天数据...")
    tickers = ["SPY", "TLT", "GLD", "USO"]
    macro_data = {}
    
    # 1.1 获取传统大类资产 (ETF)
    data = safe_yf_download(tickers, period="10d")
    for ticker in tickers:
        try:
            if ticker in data and not data[ticker].dropna().empty:
                df = data[ticker].dropna().tail(7)
                flows = []
                for date, row in df.iterrows():
                    close = float(row['Close'])
                    open_price = float(row['Open'])
                    vol = float(row['Volume'])
                    turnover = close * vol
                    
                    pct = (close - open_price) / open_price if open_price else 0
                    multiplier = max(min(pct * 10, 1.0), -1.0)
                    estimated_flow = turnover * multiplier
                    
                    flows.append({
                        "date": date.strftime('%Y-%m-%d'),
                        "net_flow_usd": round(estimated_flow, 2),
                        "close_price": round(close, 2)
                    })
                macro_data[ticker] = flows
        except Exception as e:
            print(f"解析 ETF {ticker} 失败: {e}")

    # 1.2 获取加密资产整体流入
    try:
        url = "https://stablecoins.llama.fi/stablecoincharts/all"
        res = requests.get(url, timeout=10).json()
        crypto_7d = res[-8:]
        crypto_flows = []
        for i in range(1, len(crypto_7d)):
            today_cap = float(crypto_7d[i]['totalCirculatingUSD']['peggedUSD'])
            yesterday_cap = float(crypto_7d[i-1]['totalCirculatingUSD']['peggedUSD'])
            date_str = datetime.utcfromtimestamp(int(crypto_7d[i]['date'])).strftime('%Y-%m-%d')
            crypto_flows.append({
                "date": date_str,
                "net_flow_usd": round(today_cap - yesterday_cap, 2),
                "market_cap_usd": round(today_cap, 2)
            })
        macro_data["CRYPTO"] = crypto_flows
    except Exception as e:
        print(f"抓取 Crypto 失败: {e}")

    return {"status": "success", "data": macro_data}


# ----------------------------------------------------------------------
# 看板 2：区域流动 (Layer 1 - Regional Markets)
# ----------------------------------------------------------------------
def fetch_layer1_regions_7d():
    print("正在抓取 [看板2: 区域流动] 最近 7 天数据...")
    region_data = {}
    
    # 2.1 美国市场区域概况
    try:
        spy_data = safe_yf_download("SPY", period="10d")
        if not spy_data.empty:
            df_spy = spy_data.dropna().tail(7)
            us_flows = []
            for date, row in df_spy.iterrows():
                close = float(row['Close'])
                open_p = float(row['Open'])
                pct = (close - open_p) / open_p if open_p else 0
                turnover = close * float(row['Volume'])
                flow = turnover * max(min(pct * 10, 1.0), -1.0)
                us_flows.append({"date": date.strftime('%Y-%m-%d'), "net_flow_usd": round(flow, 2)})
            region_data["US"] = us_flows
    except Exception as e:
        region_data["US"] = []

    # 2.2 中国A股区域概况
    try:
        df_a = ak.stock_market_fund_flow().tail(7)
        cn_flows = []
        for _, row in df_a.iterrows():
            cn_flows.append({
                "date": str(row['日期']),
                "net_flow_cny_100m": round(float(row['主力净流入-净额']) / 1e8, 2)
            })
        region_data["CN"] = cn_flows
    except Exception as e:
        region_data["CN"] = []

    # 2.3 中国香港区域概况
    try:
        df_hk = ak.stock_hk_ggt_historical(indicator="港股通(沪)").tail(7)
        hk_flows = []
        for _, row in df_hk.iterrows():
            hk_flows.append({
                "date": str(row['日期']),
                "net_flow_hkd_100m": round(float(row['当日买成交额']) - float(row['当日卖成交额']), 2)
            })
        region_data["HK"] = hk_flows
    except Exception as e:
        region_data["HK"] = []

    return {"status": "success", "data": region_data}


# ----------------------------------------------------------------------
# 看板 3：行业板块流动 (Layer 2 - Sector Breakdown) [支持多数据源备用切换]
# ----------------------------------------------------------------------
def fetch_sector_with_failover(sector_name):
    """行业资金流抓取灾备机制：主线路(东方财富) -> 备用线路1(同花顺) -> 备用线路2(新浪)"""
    
    # 【主线路】：东方财富 API
    try:
        df = ak.stock_sector_fund_flow_hist(symbol=sector_name).tail(7)
        if not df.empty:
            flows = []
            for _, row in df.iterrows():
                flows.append({
                    "date": str(row['日期']),
                    "net_flow_cny_100m": round(float(row['主力净流入-净额']) / 1e8, 2),
                    "pct_change": float(row['涨跌幅']),
                    "source": "Eastmoney"
                })
            return flows
    except Exception:
        print(f"  [主线路 - 东方财富] 抓取 {sector_name} 失败(502或休市)，正在自动切换至备用线路1...")

    # 【备用线路 1】：同花顺/行业大盘资金 API
    try:
        # 使用同花顺板块资金流排名代替
        df_ths = ak.stock_fund_flow_industry(symbol="即时")
        matched = df_ths[df_ths['行业'].str.contains(sector_name[:2])] # 模糊匹配行业名
        if not matched.empty:
            row = matched.iloc[0]
            # 同花顺即时接口返回单日数据，构造结构
            today_str = datetime.now().strftime('%Y-%m-%d')
            return [{
                "date": today_str,
                "net_flow_cny_100m": round(float(row['净额']) / 1e8, 2) if '净额' in row else 0.0,
                "pct_change": float(row['行业-涨跌幅']) if '行业-涨跌幅' in row else 0.0,
                "source": "10jqka_Backup"
            }]
    except Exception:
        print(f"  [备用线路 1 - 同花顺] 抓取 {sector_name} 失败，正在自动切换至备用线路2...")

    # 【备用线路 2】：新浪财经/行业大单接口
    try:
        df_sina = ak.stock_sector_fund_flow_summary(symbol="行业资金流")
        matched_sina = df_sina[df_sina['名称'].str.contains(sector_name[:2])]
        if not matched_sina.empty:
            row = matched_sina.iloc[0]
            today_str = datetime.now().strftime('%Y-%m-%d')
            return [{
                "date": today_str,
                "net_flow_cny_100m": round(float(row['净流入额']) / 1e8, 2),
                "pct_change": float(row['涨跌幅']),
                "source": "Sina_Backup"
            }]
    except Exception as e:
        print(f"  [备用线路 2 - 新浪] 抓取 {sector_name} 亦失败: {e}")

    # 所有接口皆挂掉时的安全兜底
    return []

def fetch_layer2_sectors_7d():
    print("正在抓取 [看板3: 行业板块] 最近 7 天数据 (支持多源灾备切换)...")
    core_sectors = ["半导体", "酿酒行业", "银行", "医疗器械"]
    sector_data = {}
    
    for sector in core_sectors:
        sector_data[sector] = fetch_sector_with_failover(sector)
            
    return {"status": "success", "data": sector_data}


# ----------------------------------------------------------------------
# 主程序
# ----------------------------------------------------------------------
if __name__ == "__main__":
    now_utc = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    print(f"=== 开始采集 7 天三层看板数据 ({now_utc} UTC) ===")
    
    final_json = {
        "update_time_utc": now_utc,
        "board_1_macro_assets_7d": fetch_layer0_macro_7d(),
        "board_2_regions_7d": fetch_layer1_regions_7d(),
        "board_3_sectors_7d": fetch_layer2_sectors_7d()
    }
    
    file_name = "fund_flow_data.json"
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, indent=4)
        
    print(f"\n✅ 7天三层看板数据已成功写入 {file_name}")
