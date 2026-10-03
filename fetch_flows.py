import pandas as pd
import requests
import akshare as ak
import yfinance as yf
from datetime import datetime
import json
import warnings

warnings.filterwarnings('ignore')

# ----------------------------------------------------------------------
# 看板 1：大类资产流动 (Layer 0 - Macro Assets)
# 监控标的：股市(SPY), 债市(TLT), 黄金(GLD), 原油(USO), 加密货币(Crypto)
# ----------------------------------------------------------------------
def fetch_layer0_macro_7d():
    print("正在抓取 [看板1: 大类资产] 最近 7 天数据...")
    
    # 1.1 获取传统大类资产 (ETF)
    tickers = ["SPY", "TLT", "GLD", "USO"]
    macro_data = {}
    try:
        data = yf.download(tickers, period="10d", group_by="ticker", progress=False)
        for ticker in tickers:
            if ticker in data:
                df = data[ticker].dropna().tail(7)
                flows = []
                for date, row in df.iterrows():
                    close = float(row['Close'])
                    open_price = float(row['Open'])
                    vol = float(row['Volume'])
                    turnover = close * vol
                    
                    # 伪资金流算法：(收盘-开盘)/开盘 * 放大系数 * 成交额
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
        print(f"抓取 ETF 失败: {e}")

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
# 监控区域：美国(US), 中国A股(CN), 中国香港(HK), 加密数字国(Crypto Zone)
# ----------------------------------------------------------------------
def fetch_layer1_regions_7d():
    print("正在抓取 [看板2: 区域流动] 最近 7 天数据...")
    region_data = {}
    
    # 2.1 美国市场区域概况 (用 SPY + QQQ 组合代表)
    try:
        spy = yf.download("SPY", period="10d", progress=False).dropna().tail(7)
        us_flows = []
        for date, row in spy.iterrows():
            close = float(row['Close'])
            open_p = float(row['Open'])
            pct = (close - open_p) / open_p if open_p else 0
            turnover = close * float(row['Volume'])
            flow = turnover * max(min(pct * 10, 1.0), -1.0)
            us_flows.append({"date": date.strftime('%Y-%m-%d'), "net_flow_usd": round(flow, 2)})
        region_data["US"] = us_flows
    except Exception as e:
        region_data["US"] = []

    # 2.2 中国A股区域概况 (主力资金整体流向)
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
        region_data["CN"] = "休市或无数据"

    # 2.3 中国香港区域概况 (港股通/南向资金)
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
        region_data["HK"] = "休市或无数据"

    return {"status": "success", "data": region_data}


# ----------------------------------------------------------------------
# 看板 3：行业板块流动 (Layer 2 - Sector Breakdown)
# 监控行业：A股核心赛道 (半导体, 白酒, 银行, 医疗)
# ----------------------------------------------------------------------
def fetch_layer2_sectors_7d():
    print("正在抓取 [看板3: 行业板块] 最近 7 天数据...")
    core_sectors = ["半导体", "酿酒行业", "银行", "医疗器械"]
    sector_data = {}
    
    for sector in core_sectors:
        try:
            df = ak.stock_sector_fund_flow_hist(symbol=sector).tail(7)
            flows = []
            for _, row in df.iterrows():
                flows.append({
                    "date": str(row['日期']),
                    "net_flow_cny_100m": round(float(row['主力净流入-净额']) / 1e8, 2),
                    "pct_change": float(row['涨跌幅'])
                })
            sector_data[sector] = flows
        except Exception as e:
            print(f"抓取行业 {sector} 失败: {e}")
            
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
        
    print(f"\n✅ 7天三层看板数据已完美写入 {file_name}")
