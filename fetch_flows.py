# ----------------------------------------------------------------------
# 看板 3：股市板块轮动 (美股 / A股 / 港股 细分板块)
# ----------------------------------------------------------------------
def fetch_board3_sectors_7d():
    print("正在抓取 [看板3: 股市板块轮动] (美股/A股/港股)...")
    stock_sector_data = {}

    # 1. 美股行业板块 (通过 11 大行业 ETF 倒推)
    us_sector_tickers = {
        "科技": "XLK", "医疗保健": "XLV", "金融": "XLF", 
        "能源": "XLE", "可选消费": "XLY", "工业": "XLI"
    }
    us_sectors = {}
    for name, ticker in us_sector_tickers.items():
        try:
            df = safe_yf_ticker_history(ticker)
            us_sectors[name] = calculate_estimated_flow(df)
        except Exception:
            us_sectors[name] = []
    stock_sector_data["美股"] = us_sectors

    # 2. A股行业板块 (主力资金流向 Top)
    try:
        core_a_sectors = ["半导体", "酿酒行业", "银行", "医疗器械", "新能源"]
        a_sectors = {}
        for sector in core_a_sectors:
            a_sectors[sector] = fetch_sector_with_failover(sector)
        stock_sector_data["A股"] = a_sectors
    except Exception:
        stock_sector_data["A股"] = {}

    # 3. 港股行业板块 (港股通行业分布或 ETF 替代)
    try:
        hk_sector_tickers = {"资讯科技": "3033.HK", "金融": "2838.HK", "医药": "1801.HK"}
        hk_sectors = {}
        for name, ticker in hk_sector_tickers.items():
            df = safe_yf_ticker_history(ticker)
            hk_sectors[name] = calculate_estimated_flow(df)
        stock_sector_data["港股"] = hk_sectors
    except Exception:
        stock_sector_data["港股"] = {}

    return {"status": "success", "data": stock_sector_data}
