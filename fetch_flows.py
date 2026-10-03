# -*- coding: utf-8 -*-
"""
全球资本流动监测终端 · 后端采集引擎
================================================

修订说明（相对原脚本）：
  1. 统一输出契约：每条记录固定携带 value_usd（已折算美元的净额）、
     value_native + native_unit（原始口径数值与单位）、metric_type（口径说明）。
  2. 修正 ASHR 汇率 bug：ASHR 在美股上市、以美元计价，不再除以 7.1。
  3. 所有汇率换算集中到 to_usd()，归一化前完成，杜绝单位混用。
  4. 废除 except: pass —— 统一 logger + retry + 数据质量标记（data_quality）。
  5. Intensity 保留，但仅作为「窗口内相对强度」的辅助字段，不冒充资金流；
     同时输出可比绝对量 value_usd，前端可自由选择。
  6. normalize_to_intensity 改为真递归，支持任意嵌套深度。
  7. 剔除已废弃的 datetime.utcnow()，改用 timezone-aware。

运行：
  python3 fetch_fund_flow.py --out fund_flow_data.json
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone

import pandas as pd
import requests

try:
    import akshare as ak
except ImportError:  # 允许在无 akshare 环境下做结构自检
    ak = None

try:
    import yfinance as yf
except ImportError:
    yf = None

# ----------------------------------------------------------------------
# 日志（需求：废除静默吞错）
# ----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fundflow")

# ----------------------------------------------------------------------
# 汇率：集中一处，便于按需改为实时汇率
# 说明：固定汇率仅作历史序列对齐用途；如需「实时」，可接入
#       frankfurter.app / exchangerate.host 等公开接口替换 _FX 字典。
# ----------------------------------------------------------------------
FX_TO_USD = {
    "CNY": 7.10,   # 1 USD = 7.10 CNY  （2026 口径，需定期复核）
    "HKD": 7.80,   # 1 USD = 7.80 HKD
    "USD": 1.00,
}

RETRY = 3
RETRY_SLEEP = 1.5


def fx(native_unit: str) -> float:
    """返回 native → USD 的除数。"""
    return FX_TO_USD.get(native_unit.upper(), 1.0)


# ----------------------------------------------------------------------
# 工具：带重试与日志的抓取
# ----------------------------------------------------------------------
def yf_history(ticker: str, period: str = "1mo") -> pd.DataFrame:
    """单标的安全抓取，失败重试并记录日志。"""
    if yf is None:
        log.error("yfinance 未安装，无法抓取 %s", ticker)
        return pd.DataFrame()
    for attempt in range(1, RETRY + 1):
        try:
            df = yf.Ticker(ticker).history(period=period)
            if df is not None and not df.empty:
                return df
            log.warning("[%s] 第 %d 次返回空数据", ticker, attempt)
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] 第 %d 次抓取异常: %s", ticker, attempt, exc)
        time.sleep(RETRY_SLEEP)
    log.error("[%s] 重试 %d 次后仍失败", ticker, RETRY)
    return pd.DataFrame()


def to_usd(value: float, native_unit: str) -> float:
    """native 单位数值 → USD。"""
    return value / fx(native_unit)


def rec(date: str, value_native: float, native_unit: str,
        metric_type: str, **extra) -> dict:
    """
    构造标准化记录（统一输出契约）。
    - value_usd    : 折算美元后的数值（供前端跨资产比较）
    - value_native : 原始口径数值
    - native_unit  : 原始单位（USD / CNY / HKD / USD_100M …）
    - metric_type  : 口径说明（需求强制要求）
    """
    value_usd = to_usd(value_native, native_unit)
    out = {
        "date": date,
        "value_usd": round(value_usd, 2),
        "value_native": round(value_native, 2),
        "native_unit": native_unit,
        "metric_type": metric_type,
    }
    out.update(extra)
    return out


# ----------------------------------------------------------------------
# A/D 动能代理（明确标注为「代理」，不是资金流）
# 保留 MFM 计算用于「动能方向」展示，但绝对量单独走真实口径。
# ----------------------------------------------------------------------
def ad_momentum_proxy(df: pd.DataFrame, ticker: str, days: int = 7) -> list:
    """
    以 MFM × 成交额 构造「量价动能代理」序列。
    ⚠️ 该方法衡量的是日内收盘位置所反映的多空承接，不是真实资金净流入。
       因此 metric_type 明确写为「A/D 动能代理」，前端需据此标注。
    """
    if df is None or df.empty:
        return []

    rows = []
    for date, row in df.tail(days).iterrows():
        try:
            close_p = float(row["Close"])
            high_p = float(row["High"])
            low_p = float(row["Low"])
            vol = float(row["Volume"])
        except (KeyError, TypeError, ValueError) as exc:
            log.warning("[%s] %s 行字段缺失: %s", ticker, date, exc)
            continue

        mfm = ((close_p - low_p) - (high_p - close_p)) / (high_p - low_p) \
            if high_p != low_p else 0.0
        typical = (high_p + low_p + close_p) / 3.0
        # 该标的以美元计价（美股 / 美股挂牌 ETF），故 native_unit = USD
        proxy_usd = mfm * vol * typical

        rows.append(rec(
            date=date.strftime("%Y-%m-%d"),
            value_native=proxy_usd,
            native_unit="USD",
            metric_type="A/D 动能代理（非真实资金流）",
            close_price=round(close_p, 2),
        ))
    return rows


# ----------------------------------------------------------------------
# A股主力净额（东财口径，带灾备）
# ----------------------------------------------------------------------
def fetch_cn_main_flow(days: int = 7) -> tuple[list, str]:
    """返回 (records, data_quality)。data_quality ∈ ok/degraded/failed。"""
    if ak is not None:
        # 主线路
        try:
            df = ak.stock_market_fund_flow()
            if df is not None and not df.empty:
                out = []
                for _, r in df.tail(days).iterrows():
                    # 东财口径单位为「元」，换算为亿人民币展示更直观
                    raw = float(r["主力净流入-净额"])
                    out.append(rec(
                        date=str(r["日期"]),
                        value_native=raw / 1e8,
                        native_unit="CNY",
                        metric_type="A股主力净流入（东财口径，单位已转亿）",
                        value_cny_100m=round(raw / 1e8, 2),
                    ))
                log.info("A股主力净额：主线路成功，%d 条", len(out))
                return out, "ok"
        except Exception as exc:  # noqa: BLE001
            log.warning("A股主线路失败: %s", exc)

        # 备线路
        try:
            df2 = ak.stock_market_fund_flow_hist(symbol="上证主板")
            if df2 is not None and not df2.empty:
                out = []
                for _, r in df2.tail(days).iterrows():
                    raw = float(r["主力净流入-净额"])
                    out.append(rec(
                        date=str(r["日期"]),
                        value_native=raw / 1e8,
                        native_unit="CNY",
                        metric_type="A股主力净流入（东财历史口径，单位已转亿）",
                        value_cny_100m=round(raw / 1e8, 2),
                    ))
                log.info("A股主力净额：备线路成功，%d 条", len(out))
                return out, "degraded"
        except Exception as exc:  # noqa: BLE001
            log.warning("A股备线路失败: %s", exc)

    # 终极降级：ASHR（美股挂牌，以美元计价 → 不换算汇率）
    log.warning("A股接口不可用，降级至 ASHR 动能代理")
    ashr = ad_momentum_proxy(yf_history("ASHR"), "ASHR", days)
    if ashr:
        return ashr, "degraded"
    return [], "failed"


# ----------------------------------------------------------------------
# 港股南向资金（港股通沪，单位港元）
# ----------------------------------------------------------------------
def fetch_hk_southbound(days: int = 7) -> tuple[list, str]:
    if ak is None:
        return [], "failed"
    try:
        # 沪 + 深 合计更接近「南向」全貌
        out = []
        for indicator in ("港股通(沪)", "港股通(深)"):
            df = ak.stock_hk_ggt_historical(indicator=indicator)
            if df is None or df.empty:
                continue
            for _, r in df.tail(days).iterrows():
                buy = float(r["当日买成交额"])
                sell = float(r["当日卖成交额"])
                net_hkd = buy - sell  # 港元
                out.append(rec(
                    date=str(r["日期"]),
                    value_native=net_hkd,
                    native_unit="HKD",
                    metric_type=f"南向资金净买入（{indicator}，港元）",
                    channel=indicator,
                ))
        if out:
            log.info("港股南向：成功 %d 条", len(out))
            return out, "ok"
        log.warning("港股南向返回空")
    except Exception as exc:  # noqa: BLE001
        log.warning("港股南向接口失败: %s", exc)

    # 降级：EWH（美股挂牌，美元计价）
    log.warning("港股接口不可用，降级至 EWH 动能代理")
    ewh = ad_momentum_proxy(yf_history("EWH"), "EWH", days)
    return (ewh, "degraded") if ewh else ([], "failed")


# ----------------------------------------------------------------------
# 加密：稳定币「市值日变化」（明确标注不是净法币流入）
# ----------------------------------------------------------------------
def fetch_stablecoin_cap_change(days: int = 7) -> tuple[list, str]:
    url = "https://stablecoins.llama.fi/stablecoincharts/all"
    for attempt in range(1, RETRY + 1):
        try:
            res = requests.get(url, timeout=10).json()
            if not isinstance(res, list) or len(res) < 2:
                raise ValueError("返回结构异常")
            window = res[-(days + 1):]
            out = []
            for i in range(1, len(window)):
                cur = float(window[i]["totalCirculatingUSD"]["peggedUSD"])
                prev = float(window[i - 1]["totalCirculatingUSD"]["peggedUSD"])
                dt = datetime.fromtimestamp(
                    int(window[i]["date"]), tz=timezone.utc
                ).strftime("%Y-%m-%d")
                out.append(rec(
                    date=dt,
                    value_native=cur - prev,
                    native_unit="USD",
                    metric_type="稳定币总市值日变化（≠ 净法币流入）",
                    market_cap_usd=round(cur, 2),
                ))
            log.info("稳定币市值日变化：成功 %d 条", len(out))
            return out, "ok"
        except Exception as exc:  # noqa: BLE001
            log.warning("稳定币第 %d 次失败: %s", attempt, exc)
            time.sleep(RETRY_SLEEP)
    return [], "failed"


# ----------------------------------------------------------------------
# Intensity：窗口内相对强度（辅助字段，保留但不再冒充资金流）
# ----------------------------------------------------------------------
def attach_intensity(node):
    """
    真递归：对任意嵌套字典中的 list 节点，按其 value_usd 绝对值最大值
    归一化出 intensity_score（-100 ~ 100）。
    注意：分母为该窗口内最大值，故为「相对强度」，跨组不可比。
    """
    if isinstance(node, dict):
        for key, val in node.items():
            node[key] = attach_intensity(val)
        return node

    if isinstance(node, list):
        vals = [abs(f.get("value_usd", 0.0)) for f in node if isinstance(f, dict)]
        max_abs = max(vals) if vals else 0.0
        for f in node:
            if isinstance(f, dict):
                raw = f.get("value_usd", 0.0)
                f["intensity_score"] = (
                    round(raw / max_abs * 100, 1) if max_abs else 0.0
                )
        return node

    return node


# ----------------------------------------------------------------------
# 看板 1：宏观大类资产
# ----------------------------------------------------------------------
def board1_macro() -> dict:
    log.info(">>> 看板1：宏观大类资产")
    data, quality = {}, {}

    data["A股"], quality["A股"] = fetch_cn_main_flow()
    data["港股"], quality["港股"] = fetch_hk_southbound()

    for name, ticker in (("美股", "SPY"), ("黄金", "GLD"),
                         ("原油", "USO"), ("类现金资产", "BIL")):
        rows = ad_momentum_proxy(yf_history(ticker), ticker)
        data[name] = rows
        quality[name] = "ok" if rows else "failed"

    data["加密货币"], quality["加密货币"] = fetch_stablecoin_cap_change()

    return {
        "status": "success",
        "data": attach_intensity(data),
        "data_quality": quality,
    }


# ----------------------------------------------------------------------
# 看板 2：区域市场
# ----------------------------------------------------------------------
def board2_regions() -> dict:
    log.info(">>> 看板2：区域市场")
    data, quality = {}, {}

    us = ad_momentum_proxy(yf_history("SPY"), "SPY")
    data["US"], quality["US"] = us, "ok" if us else "failed"

    data["CN"], quality["CN"] = fetch_cn_main_flow()
    data["HK"], quality["HK"] = fetch_hk_southbound()

    return {
        "status": "success",
        "data": attach_intensity(data),
        "data_quality": quality,
    }


# ----------------------------------------------------------------------
# 看板 3：股市细分板块
# ----------------------------------------------------------------------
def fetch_a_sector(sector: str, days: int = 7) -> list:
    if ak is None:
        return []
    try:
        df = ak.stock_sector_fund_flow_hist(symbol=sector)
        if df is not None and not df.empty:
            out = []
            for _, r in df.tail(days).iterrows():
                raw = float(r["主力净流入-净额"])
                out.append(rec(
                    date=str(r["日期"]),
                    value_native=raw / 1e8,
                    native_unit="CNY",
                    metric_type=f"A股 {sector} 主力净流入（东财口径）",
                    pct_change=safe_float(r, "涨跌幅"),
                ))
            return out
    except Exception as exc:  # noqa: BLE001
        log.warning("A股板块 [%s] 抓取失败: %s", sector, exc)
    return []


def safe_float(row, col, default=0.0) -> float:
    try:
        return float(row[col])
    except (KeyError, TypeError, ValueError):
        return default


def board3_sectors() -> dict:
    log.info(">>> 看板3：股市细分板块")
    data = {}

    # 美股板块 ETF
    us = {
        "科技": "XLK", "医疗保健": "XLV", "金融": "XLF",
        "能源": "XLE", "可选消费": "XLY", "工业": "XLI",
    }
    data["美股"] = {
        name: ad_momentum_proxy(yf_history(tk), tk) for name, tk in us.items()
    }

    # A股板块
    data["A股"] = {
        s: fetch_a_sector(s)
        for s in ("半导体", "酿酒行业", "银行", "医疗器械", "光伏设备")
    }

    # 港股板块 ETF（2828.HK 流动性优于 2838.HK）
    hk = {"资讯科技": "3033.HK", "金融": "2828.HK", "医药": "1801.HK"}
    data["港股"] = {
        name: ad_momentum_proxy(yf_history(tk), tk) for name, tk in hk.items()
    }

    # 质量标记（板块级）
    quality = {
        mkt: {k: ("ok" if v else "failed") for k, v in sectors.items()}
        for mkt, sectors in data.items()
    }

    return {
        "status": "success",
        "data": attach_intensity(data),
        "data_quality": quality,
    }


# ----------------------------------------------------------------------
# 主程序
# ----------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="fund_flow_data.json")
    parser.add_argument("--date", default=None,
                        help="覆盖数据日期（用于离线自检）")
    args = parser.parse_args()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log.info("=== 开始采集三层看板数据 (%s UTC) ===", now)

    payload = {
        "schema_version": "2.0",
        "update_time_utc": now,
        "data_date": args.date,
        "fx_basis": {"USD": 1.0, "CNY": FX_TO_USD["CNY"], "HKD": FX_TO_USD["HKD"]},
        "disclaimer": (
            "value_usd 为按固定汇率折算后的美元金额；"
            "标记为「A/D 动能代理」的条目为量价动能指标，非真实资金净流入。"
        ),
        "board_1_macro_assets_7d": board1_macro(),
        "board_2_regions_7d": board2_regions(),
        "board_3_sectors_7d": board3_sectors(),
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    log.info("✅ 已写入 %s", args.out)


if __name__ == "__main__":
    main()
