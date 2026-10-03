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
# 核心量化引擎与洗盘工具
# ----------------------------------------------------------------------
def safe_yf_ticker_history(ticker_symbol, period="1mo"):
    """单标的安全抓取，拉取 1 个月数据以确保包含充足的有效交易日"""
    for attempt in range(3):
        try:
            df = yf.Ticker(ticker_symbol).history(period=period)
            if not df.empty:
                return df
        except Exception:
            time.sleep(1.5)
    return pd.DataFrame()

def calculate_estimated_flow(df, ticker_symbol="SPY", days=7):
    """
    【终极A/D模型】基于日内高低收的真实承接力，并智能汇率换算统一至美元 (USD) 量级
    """
    flows = []
    if df.empty:
        return flows
    
    # 智能汇率识别 (确保量级在同一维度)
    exchange_rate = 1.0
    ticker_upper = ticker_symbol.upper()
    if ticker_upper.endswith('.HK'):
        exchange_rate = 7.8
    elif ticker_upper.endswith('.SS') or ticker_upper.endswith('.SZ') or ticker_upper == 'ASHR':
        exchange_rate = 7.1
        
    tail_df = df.tail(days)
    
    for date, row in tail_df.iterrows():
        close_p = float(row['Close'])
        high_p = float(row['High'])
        low_p = float(row['Low'])
        vol = float(row['Volume'])
        
        # A/D 核心公式：计算资金流向乘数
        if high_p != low_p:
            mfm = ((close_p - low_p) - (high_p - close_p)) / (high_p - low_p)
        else:
            mfm = 0.0
            
        typical_price = (high_p + low_p + close_p) / 3.0
        
        # 换算为统一的 USD 量级，消除百倍汇率与计价体系差
        estimated_flow_usd = (mfm * vol * typical_price) / exchange_rate
        
        flows.append({
            "date": date.strftime('%Y-%m-%d'),
            "net_flow_usd": round(estimated_flow_usd, 2),
            "close_price": round(close_p, 2)
        })
        
    return flows

def normalize_to_intensity(data_dict):
    """
    【全局动能洗盘】递归归一化引擎，深入所有单层字典与双层嵌套字典(Sectors)
    将所有绝对资金转为 -100 到 100 的强度分数，彻底屏蔽盘子大小干扰。
    """
    for key, val in data_dict.items():
        if isinstance(val, list):
            # 处理第一层 List (如 看板 1 和 看板 2)
            abs_flows = [abs(f.get('net_flow_usd', f.get('net_flow_cny_100m', f.get('net_flow_hkd_100m', 0)))) for f in val]
            max_abs = max(abs_flows) if abs_flows else 0
            
            for f in val:
                raw_val = f.get('net_flow_usd', f.get('net_flow_cny_100m', f.get('net_flow_hkd_100m', 0)))
                f['intensity_score'] = round((raw_val / max_abs) * 100, 1) if max_abs != 0 else 0.0
                
        elif isinstance(val, dict):
            # 处理嵌套的 Dict (如 看板 3 下的 "美股": {"科技": [...], "金融": [...]})
            for sub_key, sub_list in val.items():
                if isinstance(sub_list, list):
                    abs_flows = [abs(f.get('net_flow_usd', f.get('net_flow_cny_100m', f.get('net_flow_hkd_100m', 0)))) for f in sub_list]
                    max_abs = max(abs_flows) if abs_flows else 0
                    for f in sub_list:
                        raw_val = f.get('net_flow_usd', f.get('net_flow_cny_100m', f.get('net_flow_hkd_100m', 0)))
                        f['intensity_score'] = round((raw_val / max_abs) * 100, 1) if max_abs != 0 else 0.0

    return data_dict


# ----------------------------------------------------------------------
# A股多数据源灾备穿透抓取
# ----------------------------------------------------------------------
def fetch_cn_market_flow_multi_source():
    """A股大盘资金流穿透抓取，保障休市期也能获取 7 天完整序列"""
    try:
        df_a = ak.stock_market_fund_flow().tail(7)
        if not df_a.empty and len(df_a) >= 2:
            cn_flows = []
            for _, row in df_a.iterrows():
                cn_flows.append({
                    "date": str(row['日期']),
                    "net_flow_cny_100m": round(float(row['主力净流入-净额']) / 1e8, 2),
                    "source": "Eastmoney"
                })
            return cn_flows
    except Exception:
        pass

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

    print("  [A股-备用线路] 国内接口异常/休市，切换至美股 ASHR (沪深300) 倒推 7 天资金流...")
    ashr_df = safe_yf_ticker_history("ASHR")
    if not ashr_df.empty:
        return calculate_estimated_flow(ashr_df, "ASHR")

    return []


# ----------------------------------------------------------------------
# 看板 1：大类资产流动 (Layer 0 - Macro Assets)
# ----------------------------------------------------------------------
def fetch_board1_macro_assets_7d():
    print("正在抓取 [看板1: 大类资产] 7 大分类数据...")
    macro_data = {}

    macro_data["A股"] = fetch_cn_market_flow_multi_source()

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
        print("  [港股] 港股通主接口受阻，自动无缝切换至 EWH (香港ETF)...")
        ewh_df = safe_yf_ticker_history("EWH")
        macro_data["港股"] = calculate_estimated_flow(ewh_df, "EWH")

    try:
        spy_df = safe_yf_ticker_history("SPY")
        macro_data["美股"] = calculate_estimated_flow(spy_df, "SPY")
    except Exception:
        macro_data["美股"] = []

    try:
        gld_df = safe_yf_ticker_history("GLD")
        macro_data["黄金"] = calculate_estimated_flow(gld_df, "GLD")
    except Exception:
        macro_data["黄金"] = []

    try:
        uso_df = safe_yf_ticker_history("USO")
        macro_data["原油"] = calculate_estimated_flow(uso_df, "USO")
    except Exception:
        macro_data["原油"] = []

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
    except Exception:
        macro_data["加密货币"] = []

    try:
        bil_df = safe_yf_ticker_history("BIL")
        macro_data["类现金资产"] = calculate_estimated_flow(bil_df, "BIL")
    except Exception:
        macro_data["类现金资产"] = []

    # 注入 Intensity Score
    return {"status": "success", "data": normalize_to_intensity(macro_data)}


# ----------------------------------------------------------------------
# 看板 2：区域流动 (Layer 1 - Regional Markets)
# ----------------------------------------------------------------------
def fetch_board2_regions_7d():
    print("正在抓取 [看板2: 区域流动]...")
    region_data = {}
    
    try:
        spy_df = safe_yf_ticker_history("SPY")
        region_data["US"] = calculate_estimated_flow(spy_df, "SPY")
    except Exception:
        region_data["US"] = []

    region_data["CN"] = fetch_cn_market_flow_multi_source()

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
        region_data["HK"] = calculate_estimated_flow(ewh_df, "EWH")

    # 注入 Intensity Score
    return {"status": "success", "data": normalize_to_intensity(region_data)}


# ----------------------------------------------------------------------
# 看板 3：股市板块轮动 (Layer 2 - Stock Sectors)
# ----------------------------------------------------------------------
def fetch_sector_with_failover(sector_name):
    """A股细分行业灾备机制"""
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
    print("正在抓取 [看板3: 股市板块轮动]...")
    stock_sector_data = {}

    # 1. 美股板块
    us_sector_tickers = {
        "科技": "XLK", "医疗保健": "XLV", "金融": "XLF",
        "能源": "XLE", "可选消费": "XLY", "工业": "XLI"
    }
    us_sectors = {}
    for name, ticker in us_sector_tickers.items():
        try:
            df = safe_yf_ticker_history(ticker)
            us_sectors[name] = calculate_estimated_flow(df, ticker)
        except Exception:
            us_sectors[name] = []
    stock_sector_data["美股"] = us_sectors

    # 2. A股板块
    core_a_sectors = ["半导体", "酿酒行业", "银行", "医疗器械", "光伏设备"]
    a_sectors = {}
    for sector in core_a_sectors:
        a_sectors[sector] = fetch_sector_with_failover(sector)
    stock_sector_data["A股"] = a_sectors

    # 3. 港股板块 (金融已替换为流动性更好的 2828.HK)
    hk_sector_tickers = {
        "资讯科技": "3033.HK", "金融": "2828.HK", "医药": "1801.HK"
    }
    hk_sectors = {}
    for name, ticker in hk_sector_tickers.items():
        try:
            df = safe_yf_ticker_history(ticker)
            hk_sectors[name] = calculate_estimated_flow(df, ticker)
        except Exception:
            hk_sectors[name] = []
    stock_sector_data["港股"] = hk_sectors

    # 递归注入 Intensity Score
    return {"status": "success", "data": normalize_to_intensity(stock_sector_data)}


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
        
    print(f"\n✅ 终极版（含 A/D模型 + 全局 Intensity 强效洗盘）已成功写入 {file_name}")
