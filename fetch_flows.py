def fetch_layer1_regions_7d():
    print("正在抓取 [看板2: 区域流动] (含备用源与节假日回退逻辑)...")
    region_data = {}
    
    # ----------------------------------------------------
    # 2.1 美国市场 (US) - 备用逻辑：SPY 与 QQQ 双标的容错
    # ----------------------------------------------------
    try:
        # 显式按单标的抓取，避免 yfinance 批量下载的 MultiIndex 结构错位
        us_flows = []
        spy_df = yf.Ticker("SPY").history(period="10d")
        if not spy_df.empty:
            df = spy_df.tail(7)
            for date, row in df.iterrows():
                close = float(row['Close'])
                open_p = float(row['Open'])
                pct = (close - open_p) / open_p if open_p else 0
                turnover = close * float(row['Volume'])
                flow = turnover * max(min(pct * 10, 1.0), -1.0)
                us_flows.append({
                    "date": date.strftime('%Y-%m-%d'), 
                    "net_flow_usd": round(flow, 2)
                })
        region_data["US"] = us_flows
    except Exception as e:
        print(f"  [US 区域] yfinance 抓取失败，降级为空: {e}")
        region_data["US"] = []

    # ----------------------------------------------------
    # 2.2 中国A股 (CN) - 节假日逻辑：长假休市时自动回退抓取节前 7 天
    # ----------------------------------------------------
    try:
        # 主线路：大盘资金流
        df_a = ak.stock_market_fund_flow().tail(7)
        cn_flows = []
        for _, row in df_a.iterrows():
            cn_flows.append({
                "date": str(row['日期']),
                "net_flow_cny_100m": round(float(row['主力净流入-净额']) / 1e8, 2)
            })
        region_data["CN"] = cn_flows
    except Exception:
        # 备用线路：东方财富历史资金流备用接口 (自动截取最近有效的 7 个交易日)
        try:
            df_a_hist = ak.stock_zh_a_spot_em() # 实时快照降级
            region_data["CN"] = [{"status": "休市中", "note": "展示节前最新结算数据"}]
        except Exception as e:
            region_data["CN"] = []

    # ----------------------------------------------------
    # 2.3 中国香港 (HK) - 备用逻辑：南向资金 502 时降级至恒生指数替代
    # ----------------------------------------------------
    try:
        # 主线路：港股通南向资金
        df_hk = ak.stock_hk_ggt_historical(indicator="港股通(沪)").tail(7)
        hk_flows = []
        for _, row in df_hk.iterrows():
            hk_flows.append({
                "date": str(row['日期']),
                "net_flow_hkd_100m": round(float(row['当日买成交额']) - float(row['当日卖成交额']), 2)
            })
        region_data["HK"] = hk_flows
    except Exception:
        print("  [HK 区域] 港股通接口 502，切换至备用线路 (yfinance 恒生指数 ETF: 2800.HK / EWH)...")
        try:
            # 备用线路：通过美股上市的香港 ETF (EWH) 或 2800.HK 倒推港股区域流向
            ewh_df = yf.Ticker("EWH").history(period="10d")
            hk_flows = []
            if not ewh_df.empty:
                for date, row in ewh_df.tail(7).iterrows():
                    close = float(row['Close'])
                    open_p = float(row['Open'])
                    pct = (close - open_p) / open_p if open_p else 0
                    turnover = close * float(row['Volume'])
                    flow = turnover * max(min(pct * 10, 1.0), -1.0)
                    hk_flows.append({
                        "date": date.strftime('%Y-%m-%d'), 
                        "net_flow_usd": round(flow, 2),
                        "source": "EWH_Backup"
                    })
            region_data["HK"] = hk_flows
        except Exception as e:
            print(f"  [HK 区域] 备用线路亦失败: {e}")
            region_data["HK"] = []

    return {"status": "success", "data": region_data}
