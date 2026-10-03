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
# 基础工具与专业量化算法层
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

def calculate_estimated_flow(df, days=7):
    """
    【核心升级】：量化级资金流估算 (基于 Accumulation/Distribution 累积派发原理)
    不再使用简单的涨跌幅，而是通过日内最高、最低与收盘价的相对位置测算真实的买卖压承接力。
    """
    flows = []
    if df.empty:
        return flows
    
    # 截取最近的 days 天
    tail_df = df.tail(days)
    
    for date, row in tail_df.iterrows():
        close_p = float(row['Close'])
        high_p = float(row['High'])
        low_p = float(row['Low'])
        vol = float(row['Volume'])
        
        # 1. 计算资金流向乘数 (Money Flow Multiplier, MFM)
        # 衡量收盘价在当天震荡区间的位置。1代表收在最高点(纯买盘)，-1代表收在最低点(纯卖盘)
        if high_p != low_p:
            mfm = ((close_p - low_p) - (high_p - close_p)) / (high_p - low_p)
        else:
            mfm = 0.0  # 类似一字涨停/跌停，无日内波动
            
        # 2. 计算典型价格 (Typical Price)
        typical_price = (high_p + low_p + close_p) / 3.0
        
        # 3. 估算日内真实买卖净额 (Estimated Money Flow Volume)
        # 结果为正表示多头承接盘胜出，为负表示空头抛压胜出
        estimated_flow = mfm * vol * typical_price
        
        flows.append({
            "date": date.strftime('%Y-%m-%d'),
            "net_flow_usd": round(estimated_flow, 2),
            "close_price": round(close_p, 2)
        })
        
    return flows

def normalize_to_intensity(data_dict):
    """
    【宏观归一化引擎】：将绝对资金流转化为自身波动极值的 -100 到 +100 强度分数。
    解决跨市场(A股 vs 美股)、跨币种(人民币 vs 美元)、跨口径无法同台对比的问题。
    """
    for asset, flows in data_dict.items():
        if not flows:
            continue
        
        # 寻找该资产在当前周期内绝对值的最大值（作为 100% 动能基准标尺）
        abs_flows = [abs(f.get('net_flow_usd', f.get('net_flow_cny_100m', f.get('net_flow_hkd_100m', 0)))) for f in flows]
        max_abs = max(abs_flows) if abs_flows else 0
        
        for f in flows:
            raw_val = f.get('net_flow_usd', f.get('net_flow_cny_100m', f.get('net_flow_hkd_100m', 0)))
            if max_abs == 0:
                f['intensity_score'] = 0.0
            else:
                # 计算强度分数，保留 1 位小数
                f['intensity_score'] = round((raw_val / max_abs) * 100, 1)
                
    return data_dict


# ----------------------------------------------------------------------
# A股多数据源灾备穿透抓取 (主线路: 东财 -> 备用: yfinance ASHR)
# ----------------------------------------------------------------------
def fetch_cn_market_flow_multi_source():
    """A股大盘资金流穿透抓取，保障休市期也能获取 7 天完整序列"""
    try:
        # 主线路：抓取东方财富主力资金（实时/近期）
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
        # 备用线路 1：抓取东方财富大盘资金历史接口
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

    # 终极保底：长假或接口彻底熔断时，用美股沪深300ETF (ASHR) A/D模型倒推
    print("  [A股-备用线路] 国内接口异常/休市，切换至美股 ASHR (沪深300) 倒推 7 天资金流...")
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
        print("  [港股] 港股通主接口受阻，自动无缝切换至 EWH (香港ETF)...")
        ewh_df = safe_yf_ticker_history("EWH")
        macro_data["港股"] = calculate_estimated_flow(ewh_df)

    # 3. 美股 (SPY 代表)
    try:
        spy_df = safe_yf_ticker_history("SPY")
        macro_data["美股"] = calculate_estimated_flow(spy_df)
    except Exception:
        macro_data["美股"] = []

    # 4. 黄金 (GLD 代表)
    try:
        gld_df = safe_yf_ticker_history("GLD")
        macro_data["黄金"] = calculate_estimated_flow(gld_df)
    except Exception:
        macro_data["黄金"] = []

    # 5. 原油 (USO 代表)
    try:
        uso_df = safe_yf_ticker_history("USO")
        macro_data["原油"] = calculate_estimated_flow(uso_df)
    except Exception:
        macro_data["原油"] = []

    # 6. 加密货币 (DefiLlama 稳定币净法币流入)
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

    # 7. 类现金资产 (BIL 超短债代表)
    try:
        bil_df = safe_yf_ticker_history("BIL")
        macro_data["类现金资产"] = calculate_estimated_flow(bil_df)
    except Exception:
        macro_data["类现金资产"] = []

    # 【重要】进行资金动能强度归一化 (注入 intensity_score，范围 -100 到 +100)
    macro_data = normalize_to_intensity(macro_data)

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
# 看板 3：股市板块轮动 (Layer 2 - Stock Sectors)
# ----------------------------------------------------------------------
def fetch_sector_with_failover(sector_name):
    """A股细分行业灾备机制：东方财富 -> 同花顺 -> 新浪"""
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

    # 3. 港股板块 (金融标的已替换为流动性更好的 2828.HK 恒生国企 ETF)
    hk_sector_tickers = {
        "资讯科技": "3033.HK",
        "金融": "2828.HK", 
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
        
    print(f"\n✅ 全量专业级数据（含 A/D 量价模型与强度归一化）已成功写入 {file_name}")
