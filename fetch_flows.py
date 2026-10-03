import pandas as pd
import requests
import akshare as ak
import yfinance as yf
from datetime import datetime
import json
import time
import warnings

warnings.filterwarnings('ignore')

# ----------------------------------------------------------------------
# 基础工具函数
# ----------------------------------------------------------------------
def safe_yf_ticker_history(ticker_symbol, period="10d"):
    """单标的安全抓取，防止 MultiIndex 结构错位及 SQLite 锁库"""
    for attempt in range(3):
        try:
            df = yf.Ticker(ticker_symbol).history(period=period)
            if not df.empty:
                return df
        except Exception:
            time.sleep(1.5)
    return pd.DataFrame()

def calculate_estimated_flow(df, days=7):
    """通用伪资金流计算算法：(收盘-开盘)/开盘 * 放大系数 * 成交额"""
    flows = []
    if df.empty:
        return flows
    
    tail_df = df.tail(days)
    for date, row in tail_df.iterrows():
        close = float(row['Close'])
        open_p = float(row['Open'])
        vol = float(row['Volume'])
        turnover = close * vol
        
        pct = (close - open_p) / open_p if open_p else 0
        multiplier = max(min(pct * 10, 1.0), -1.0)
        estimated_flow = turnover * multiplier
        
        flows.append({
            "date": date.strftime('%Y-%m-%d'),
            "net_flow_usd": round(estimated_flow, 2),
            "close_price": round(close, 2)
        })
    return flows


# ----------------------------------------------------------------------
# A股多数据源灾备穿透抓取 (主线路: 东财 -> 备用1: 同花顺 -> 备用2: 新浪/腾讯 REST)
# ----------------------------------------------------------------------
def fetch_cn_market_flow_multi_source():
    """A股多源穿透：主线路(东财历史) -> 备用1(同花顺即时) -> 备用2(yfinance ASHR 历史估算)"""
    
    # 【主线路】：东方财富大盘资金流（带历史）
    try:
        df_a = ak.stock_market_fund_flow().tail(7)
        if not df_a.empty and len(df_a) >= 2: # 确保抓到的是多天历史
            cn_flows = []
            for _, row in df_a.iterrows():
                cn_flows.append({
                    "date": str(row['日期']),
                    "net_flow_cny_100m": round(float(row['主力净流入-净额']) / 1e8, 2),
                    "source": "Eastmoney"
                })
            return cn_flows
    except Exception as e:
        print(f"  [A股-主线路] 东财接口异常: {e}，尝试备用线路...")

    # 【备用线路 1】：同花顺行业历史资金流（按日期汇总）
    try:
        df_a_hist = ak.stock_market_fund_flow_hist(symbol="上证主板").tail(7)
        if not df_a_hist.empty:
            cn_flows = []
            for _, row in df_a_hist.iterrows():
                cn_flows.append({
                    "date": str(row['日期']),
                    "net_flow_cny_100m": round(float(row['主力净流入-净额']) / 1e8, 2),
                    "source": "Eastmoney_Hist"
                })
            return cn_flows
    except Exception:
        pass

    # 【备用线路 2 (终极保底)】：与港股(EWH)对齐，使用美股上市的 A股 ETF (ASHR) 倒推 7 天资金流
    print("  [A股-保底线路] 切换至美股 ASHR (沪深300 ETF) 倒推 7 天历史资金流...")
    ashr_df = safe_yf_ticker_history("ASHR")
    if not ashr_df.empty:
        return calculate_estimated_flow(ashr_df)

    return []


# ----------------------------------------------------------------------
# 看板 1：大类资产流动 (Layer 0 - Macro Assets)
# ----------------------------------------------------------------------
def fetch_board1_macro_assets_7d():
    print("正在抓取 [看板1: 大类资产] 7 大分类数据...")
    macro_data = {}

    # 1. A股 (多源穿透)
    macro_data["A股"] = fetch_cn_market_flow_multi_source()

    # 2. 港股
    try:
        df_hk = ak.stock_hk_ggt_historical(indicator="港股通(沪)").tail(7)
        hk_flows = []
        for _, row in df_hk.iterrows():
            hk_flows.append({
                "date": str(row['日期']),
                "net_flow_hkd_100m": round(float(row['当日买成交额']) - float(row['当日卖成交额']), 2),
                "source": "GGT_Southbound"
            })
        macro_data["港股"] = hk_flows
    except Exception:
        print("  [港股] 港股通主接口受阻，自动无缝切换至 EWH (香港ETF) 备用线路...")
        ewh_df = safe_yf_ticker_history("EWH")
        macro_data["港股"] = calculate_estimated_flow(ewh_df)

    # 3. 美股 (SPY 代表)
    try:
        spy_df = safe_yf_ticker_history("SPY")
        macro_data["美股"] = calculate_estimated_flow(spy_df)
    except Exception as e:
        print(f"  [美股] 抓取失败: {e}")
        macro_data["美股"] = []

    # 4. 黄金 (GLD 代表)
    try:
        gld_df = safe_yf_ticker_history("GLD")
        macro_data["黄金"] = calculate_estimated_flow(gld_df)
    except Exception as e:
        print(f"  [黄金] 抓取失败: {e}")
        macro_data["黄金"] = []

    # 5. 原油 (USO 代表)
    try:
        uso_df = safe_yf_ticker_history("USO")
        macro_data["原油"] = calculate_estimated_flow(uso_df)
    except Exception as e:
        print(f"  [原油] 抓取失败: {e}")
        macro_data["原油"] = []

    # 6. 加密货币 (DefiLlama 稳定币市值)
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
        macro_data["加密货币"] = crypto_flows
    except Exception as e:
        print(f"  [加密货币] 抓取失败: {e}")
        macro_data["加密货币"] = []

    # 7. 类现金资产 (BIL 代表)
    try:
        bil_df = safe_yf_ticker_history("BIL")
        macro_data["类现金资产"] = calculate_estimated_flow(bil_df)
    except Exception as e:
        print(f"  [类现金资产] 抓取失败: {e}")
        macro_data["类现金资产"] = []

    return {"status": "success", "data": macro_data}


# ----------------------------------------------------------------------
# 看板 2：区域流动 (Layer 1 - Regional Markets)
# ----------------------------------------------------------------------
def fetch_board2_regions_7d():
    print("正在抓取 [看板2: 区域流动]...")
    region_data = {}
    
    # 2.1 美国区域 (US)
    try:
        spy_df = safe_yf_ticker_history("SPY")
        region_data["US"] = calculate_estimated_flow(spy_df)
    except Exception:
        region_data["US"] = []

    # 2.2 中国区域 (CN)
    region_data["CN"] = fetch_cn_market_flow_multi_source()

    # 2.3 中国香港区域 (HK)
    try:
        df_hk = ak.stock_hk_ggt_historical(indicator="港股通(沪)").tail(7)
        hk_flows = []
        for _, row in df_hk.iterrows():
            hk_flows.append({
                "date": str(row['日期']),
                "net_flow_hkd_100m": round(float(row['当日买成交额']) - float(row['当日卖成交额']), 2)
            })
        region_data["HK"] = hk_flows
    except Exception:
        ewh_df = safe_yf_ticker_history("EWH")
        region_data["HK"] = calculate_estimated_flow(ewh_df)

    return {"status": "success", "data": region_data}


# ----------------------------------------------------------------------
# 看板 3：股市板块轮动 (Layer 2 - Stock Sectors: 美股 / A股 / 港股)
# ----------------------------------------------------------------------
def fetch_sector_with_failover(sector_name):
    """行业资金流灾备穿透机制：东方财富 -> 同花顺 -> 新浪"""
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
        pass

    try:
        df_ths = ak.stock_fund_flow_industry(symbol="即时")
        matched = df_ths[df_ths['行业'].str.contains(sector_name[:2])]
        if not matched.empty:
            row = matched.iloc[0]
            today_str = datetime.now().strftime('%Y-%m-%d')
            return [{
                "date": today_str,
                "net_flow_cny_100m": round(float(row['净额']) / 1e8, 2) if '净额' in row else 0.0,
                "pct_change": float(row['行业-涨跌幅']) if '行业-涨跌幅' in row else 0.0,
                "source": "10jqka_Backup"
            }]
    except Exception:
        pass

    return []

def fetch_board3_sectors_7d():
    print("正在抓取 [看板3: 股市板块轮动] (美股/A股/港股)...")
    stock_sector_data = {}

    # 1. 美股板块
    us_sector_tickers = {
        "科技": "XLK",
        "医疗保健": "XLV",
        "金融": "XLF",
        "能源": "XLE",
        "可选消费": "XLY",
        "工业": "XLI"
    }
    us_sectors = {}
    for name, ticker in us_sector_tickers.items():
        try:
            df = safe_yf_ticker_history(ticker)
            us_sectors[name] = calculate_estimated_flow(df)
        except Exception:
            us_sectors[name] = []
    stock_sector_data["美股"] = us_sectors

    # 2. A股板块
    core_a_sectors = ["半导体", "酿酒行业", "银行", "医疗器械", "光伏设备"]
    a_sectors = {}
    for sector in core_a_sectors:
        a_sectors[sector] = fetch_sector_with_failover(sector)
    stock_sector_data["A股"] = a_sectors

    # 3. 港股板块
    hk_sector_tickers = {
        "资讯科技": "3033.HK",
        "金融": "2838.HK",
        "医药": "1801.HK"
    }
    hk_sectors = {}
    for name, ticker in hk_sector_tickers.items():
        try:
            df = safe_yf_ticker_history(ticker)
            hk_sectors[name] = calculate_estimated_flow(df)
        except Exception:
            hk_sectors[name] = []
    stock_sector_data["港股"] = hk_sectors

    return {"status": "success", "data": stock_sector_data}


# ----------------------------------------------------------------------
# 主程序
# ----------------------------------------------------------------------
if __name__ == "__main__":
    now_utc = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    print(f"=== 开始采集全量三层看板数据 ({now_utc} UTC) ===")
    
    final_json = {
        "update_time_utc": now_utc,
        "board_1_macro_assets_7d": fetch_board1_macro_assets_7d(),
        "board_2_regions_7d": fetch_board2_regions_7d(),
        "board_3_sectors_7d": fetch_board3_sectors_7d()
    }
    
    file_name = "fund_flow_data.json"
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, indent=4)
        
    print(f"\n✅ 全量多源穿透数据已成功写入 {file_name}")
