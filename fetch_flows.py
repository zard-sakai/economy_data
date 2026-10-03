# -*- coding: utf-8 -*-
"""
全球资本流动监测终端 · 后端采集引擎
================================================

本版修订（v3）解决的问题：

  A. 【接口名不存在】原脚本调用了两个 akshare 中根本不存在的函数：
       - ak.stock_market_fund_flow_hist  → 从未存在，A股备线路是死代码
       - ak.stock_hk_ggt_historical      → 从未存在，港股线路是死代码
     本版删除死代码，改用真实存在的 stock_hsgt_hist_em(symbol=...) 取南向资金。

  B. 【push2his 域名被封锁】ak.stock_market_fund_flow() 与
     ak.stock_sector_fund_flow_hist() 均走 push2his.eastmoney.com，
     该域名在部分出口 IP（含 GitHub runner）会被直接掐断连接
     （RemoteDisconnected），加 Referer 无效 → 判定为 IP 级封锁。
     本版对这两个接口做「探测式处理」：不通时明确标记 failed。

  C. 【降级不得静默产出】原版降级到 MFM 动能代理后，记录仍以正常字段
     写入 JSON，读者无法分辨真伪。本版：
       - 每条记录强制携带 metric_type（口径说明）；
       - 每条记录强制携带 is_proxy 布尔位（True = 动能代理，非真实资金流）；
       - 核心口径不达标时直接 SystemExit(1)，让 CI 红着退出。
         宁可不产出，也不产出「看着正常的假数据」。

  D. 沿用上一版已修好的部分：
       - ASHR 汇率 bug（美股挂牌 ETF 以美元计价，不再除以 7.1）
       - 全局请求节流 _throttle() + 指数退避重试
       - attach_intensity 真递归
       - 废弃 datetime.utcnow()

输出契约（schema v3.0）：
    {
      "schema_version": "3.0",
      "update_time_utc": "...",
      "fx_basis": {...},
      "disclaimer": "...",
      "data_integrity": {            # 新增：整体数据可信度
          "all_real": true/false,    # 是否全部为真实资金流
          "proxy_items": [...],      # 哪些条目用了动能代理
          "degraded": true/false,    # 核心口径是否降级
          "core_failures": [...],
          "notes": [...]
      },
      "board_1_macro_assets_7d": {...},
      "board_2_regions_7d": {...},
      "board_3_sectors_7d": {...}
    }

每条记录字段：
    {
      "date": "YYYY-MM-DD",
      "value_usd": float,       # 折算美元
      "value_native": float,    # 原始单位数值
      "native_unit": "CNY/HKD/USD",
      "metric_type": "...",     # 口径说明（必填）
      "is_proxy": bool,         # True = 动能代理，非真实资金流
      ...其他附加字段
    }

运行：
  python3 fetch_flows.py --out fund_flow_data.json
  python3 fetch_flows.py --out fund_flow_data.json --allow-degraded   # 调试用
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
# ----------------------------------------------------------------------
FX_TO_USD = {
    "CNY": 7.10,   # 1 USD = 7.10 CNY
    "HKD": 7.80,   # 1 USD = 7.80 HKD
    "USD": 1.00,
}

RETRY = 4                 # 重试次数（含首次）
RETRY_SLEEP = 2.0         # 指数退避基数（秒）
REQUEST_INTERVAL = 1.2    # 相邻外网请求最小间隔（秒），降低被限流概率
_last_request_ts = 0.0

# 全局采集痕迹：记录哪些条目用了代理，供最终 data_integrity 汇总
_PROXY_TRACE: list = []
_NOTES: list = []


def _mark_proxy(where: str) -> None:
    """登记一处代理降级，供最终汇总。"""
    if where not in _PROXY_TRACE:
        _PROXY_TRACE.append(where)


def _mark_note(msg: str) -> None:
    if msg not in _NOTES:
        _NOTES.append(msg)


def _throttle() -> None:
    """全局请求节流：保证相邻外网请求间隔 >= REQUEST_INTERVAL 秒。"""
    global _last_request_ts
    delta = time.time() - _last_request_ts
    if delta < REQUEST_INTERVAL:
        time.sleep(REQUEST_INTERVAL - delta)
    _last_request_ts = time.time()


def _is_rate_limited(exc: Exception) -> bool:
    """识别限流类异常（Yahoo 429）。"""
    msg = str(exc).lower()
    return "too many requests" in msg or "429" in msg or "rate limit" in msg


def _is_connection_killed(exc: Exception) -> bool:
    """
    识别「连接被对端直接掐断」类错误。
    典型：push2his.eastmoney.com 对部分出口 IP 的封锁。
    """
    msg = str(exc).lower()
    return (
        "remote end closed connection" in msg
        or "connection aborted" in msg
        or "remotedisconnected" in msg
        or "connection reset" in msg
    )


def fx(native_unit: str) -> float:
    """返回 native -> USD 的除数。"""
    return FX_TO_USD.get(native_unit.upper(), 1.0)


# ----------------------------------------------------------------------
# 工具：带「全局节流 + 指数退避」的抓取
# ----------------------------------------------------------------------
def yf_history(ticker: str, period: str = "1mo") -> pd.DataFrame:
    """单标的安全抓取：全局节流 + 指数退避重试，失败记录日志。"""
    if yf is None:
        log.error("yfinance 未安装，无法抓取 %s", ticker)
        return pd.DataFrame()
    for attempt in range(1, RETRY + 1):
        _throttle()
        try:
            df = yf.Ticker(ticker).history(period=period)
            if df is not None and not df.empty:
                return df
            log.warning("[%s] 第 %d 次返回空数据", ticker, attempt)
        except Exception as exc:  # noqa: BLE001
            limited = _is_rate_limited(exc)
            log.warning("[%s] 第 %d 次抓取异常%s: %s",
                        ticker, attempt, "（限流）" if limited else "", exc)
            if limited:
                # 被限流时退避更久，避免连续打空
                time.sleep(RETRY_SLEEP * (2 ** attempt) + 3.0)
                continue
        time.sleep(RETRY_SLEEP * attempt)
    log.error("[%s] 重试 %d 次后仍失败", ticker, RETRY)
    return pd.DataFrame()


def to_usd(value: float, native_unit: str) -> float:
    """native 单位数值 -> USD。"""
    return value / fx(native_unit)


def rec(date: str, value_native: float, native_unit: str,
        metric_type: str, is_proxy: bool = False, **extra) -> dict:
    """
    构造标准化记录（统一输出契约）。
    - value_usd    : 折算美元后的数值（供前端跨资产比较）
    - value_native : 原始口径数值
    - native_unit  : 原始单位（USD / CNY / HKD）
    - metric_type  : 口径说明（强制要求）
    - is_proxy     : True 表示这是动能代理，不是真实资金流
    """
    value_usd = to_usd(value_native, native_unit)
    out = {
        "date": date,
        "value_usd": round(value_usd, 2),
        "value_native": round(value_native, 2),
        "native_unit": native_unit,
        "metric_type": metric_type,
        "is_proxy": is_proxy,
    }
    out.update(extra)
    return out


# ----------------------------------------------------------------------
# A/D 动能代理（明确标注为「代理」，不是资金流）
# ----------------------------------------------------------------------
def ad_momentum_proxy(df: pd.DataFrame, ticker: str, days: int = 7) -> list:
    """
    以 MFM x 成交额 构造「量价动能代理」序列。
    该方法衡量的是日内收盘位置所反映的多空承接，不是真实资金净流入。
    metric_type 明确写为「A/D 动能代理」，且 is_proxy=True。
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
            is_proxy=True,
            close_price=round(close_p, 2),
        ))
    return rows


# ----------------------------------------------------------------------
# akshare 接口存在性守卫
# ----------------------------------------------------------------------
def _ak_has(func_name: str) -> bool:
    """防止调用不存在的方法导致 AttributeError（原脚本的两个死接口就是这么来的）。"""
    return ak is not None and hasattr(ak, func_name)


# ----------------------------------------------------------------------
# A股主力净额（东财口径）
# 注意：ak.stock_market_fund_flow() 走 push2his.eastmoney.com，
#       该域名对部分出口 IP 直接掐断连接，且加 Referer 无效（IP 级封锁）。
#       拿不到就明确 failed，不再用动能代理冒充。
# ----------------------------------------------------------------------
def fetch_cn_main_flow(days: int = 7):
    """
    返回 (records, data_quality)。data_quality ∈ ok/degraded/failed。
    """
    if not _ak_has("stock_market_fund_flow"):
        log.error("akshare 缺少 stock_market_fund_flow，A股主线不可用")
        return [], "failed"

    try:
        _throttle()
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
                    is_proxy=False,
                    value_cny_100m=round(raw / 1e8, 2),
                ))
            log.info("A股主力净额：成功，%d 条", len(out))
            return out, "ok"
    except Exception as exc:  # noqa: BLE001
        if _is_connection_killed(exc):
            log.error(
                "A股主线被连接层掐断（push2his.eastmoney.com 疑似 IP 级封锁）: %s",
                exc,
            )
            _mark_note(
                "A股主力净额不可用：push2his.eastmoney.com 连接被掐断，"
                "加 Referer 无效，属出口 IP 级封锁，需更换数据源。"
            )
        else:
            log.warning("A股主线失败: %s", exc)

    return [], "failed"


# ----------------------------------------------------------------------
# 港股南向资金（真实接口：stock_hsgt_hist_em）
# 单位：亿元（人民币），非港元。此接口实测可用（2014-11-17 至今 2700+ 条）。
# 合法 symbol: {"北向资金","沪股通","深股通","南向资金","港股通沪","港股通深"}
# 注意：北向资金（沪股通/深股通）自 2024 年起官方停止披露，返回全 NaN，不可用。
# ----------------------------------------------------------------------
def fetch_hk_southbound_total(days: int = 7):
    """
    南向资金「合计」口径（推荐）。
    与「南向资金」这一市场概念一一对应，避免沪/深叠加造成重复计数。
    """
    if not _ak_has("stock_hsgt_hist_em"):
        log.error("akshare 缺少 stock_hsgt_hist_em，南向资金不可用")
        return [], "failed"
    try:
        _throttle()
        df = ak.stock_hsgt_hist_em(symbol="南向资金")
        if df is not None and not df.empty:
            out = []
            for _, r in df.tail(days).iterrows():
                net = r.get("当日成交净买额")
                if net is None or pd.isna(net):
                    continue
                out.append(rec(
                    date=str(r["日期"]),
                    value_native=float(net),
                    native_unit="CNY",
                    metric_type="南向资金净买入（合计，亿元）",
                    is_proxy=False,
                    channel="南向资金",
                ))
            if out:
                log.info("南向资金（合计）：成功 %d 条", len(out))
                return out, "ok"
        log.warning("南向资金（合计）返回空")
    except Exception as exc:  # noqa: BLE001
        log.warning("南向资金（合计）失败: %s", exc)

    _mark_note("南向资金（合计）不可用（stock_hsgt_hist_em）")
    return [], "failed"


# ----------------------------------------------------------------------
# 加密：稳定币「市值日变化」（明确标注不是净法币流入）
# ----------------------------------------------------------------------
def fetch_stablecoin_cap_change(days: int = 7):
    url = "https://stablecoins.llama.fi/stablecoincharts/all"
    for attempt in range(1, RETRY + 1):
        try:
            _throttle()
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
                    is_proxy=False,
                    market_cap_usd=round(cur, 2),
                ))
            log.info("稳定币市值日变化：成功 %d 条", len(out))
            return out, "ok"
        except Exception as exc:  # noqa: BLE001
            log.warning("稳定币第 %d 次失败: %s", attempt, exc)
            time.sleep(RETRY_SLEEP)
    _mark_note("稳定币数据不可用（stablecoins.llama.fi）")
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

    # A股：真实资金流，拿不到就 failed（不用代理冒充）
    data["A股"], quality["A股"] = fetch_cn_main_flow()

    # 港股：真实南向资金（合计口径）
    data["港股"], quality["港股"] = fetch_hk_southbound_total()

    # 美股 / 黄金 / 原油 / 类现金：标的是美股挂牌 ETF，
    # 无 A股式官方份额接口，只能用 A/D 动能代理（明确标注 is_proxy=true）
    for name, ticker in (("美股", "SPY"), ("黄金", "GLD"),
                         ("原油", "USO"), ("类现金资产", "BIL")):
        rows = ad_momentum_proxy(yf_history(ticker), ticker)
        data[name] = rows
        quality[name] = "ok" if rows else "failed"
        if rows:
            _mark_proxy("board1.%s(%s)" % (name, ticker))

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
    if us:
        _mark_proxy("board2.US(SPY)")

    data["CN"], quality["CN"] = fetch_cn_main_flow()
    data["HK"], quality["HK"] = fetch_hk_southbound_total()

    return {
        "status": "success",
        "data": attach_intensity(data),
        "data_quality": quality,
    }


# ----------------------------------------------------------------------
# 看板 3：股市细分板块
# ----------------------------------------------------------------------
def safe_float(row, col, default=0.0) -> float:
    try:
        v = row[col]
        if v is None or pd.isna(v):
            return default
        return float(v)
    except (KeyError, TypeError, ValueError):
        return default


def fetch_a_sector(sector: str, days: int = 7) -> list:
    """
    A股板块资金流。走 push2his.eastmoney.com，同样受 IP 级封锁影响。
    拿不到就返回空列表，由上层标 failed。
    """
    if not _ak_has("stock_sector_fund_flow_hist"):
        return []
    try:
        _throttle()
        df = ak.stock_sector_fund_flow_hist(symbol=sector)
        if df is not None and not df.empty:
            out = []
            for _, r in df.tail(days).iterrows():
                raw = float(r["主力净流入-净额"])
                out.append(rec(
                    date=str(r["日期"]),
                    value_native=raw / 1e8,
                    native_unit="CNY",
                    metric_type="A股 %s 主力净流入（东财口径）" % sector,
                    is_proxy=False,
                    pct_change=safe_float(r, "涨跌幅"),
                ))
            return out
    except Exception as exc:  # noqa: BLE001
        if _is_connection_killed(exc):
            _mark_note("A股板块 [%s] 被连接层掐断（push2his IP 级封锁）" % sector)
        else:
            log.warning("A股板块 [%s] 抓取失败: %s", sector, exc)
    return []


def board3_sectors() -> dict:
    log.info(">>> 看板3：股市细分板块")
    data = {}

    # 美股板块 ETF（A/D 动能代理，明确标注）
    us = {
        "科技": "XLK", "医疗保健": "XLV", "金融": "XLF",
        "能源": "XLE", "可选消费": "XLY", "工业": "XLI",
    }
    data["美股"] = {
        name: ad_momentum_proxy(yf_history(tk), tk) for name, tk in us.items()
    }
    for name, tk in us.items():
        if data["美股"][name]:
            _mark_proxy("board3.美股.%s(%s)" % (name, tk))

    # A股板块（真实资金流，受 push2his 封锁影响）
    data["A股"] = {
        s: fetch_a_sector(s)
        for s in ("半导体", "酿酒行业", "银行", "医疗器械", "光伏设备")
    }

    # 港股板块 ETF：注意这些是「港股本土标的」（.HK 后缀），
    # 与 A股上市的跨境 QDII ETF（51xxxx/16xxxx）性质不同，勿混用。
    hk = {"资讯科技": "3033.HK", "金融": "2828.HK", "医药": "1801.HK"}
    data["港股"] = {
        name: ad_momentum_proxy(yf_history(tk), tk) for name, tk in hk.items()
    }
    for name, tk in hk.items():
        if data["港股"][name]:
            _mark_proxy("board3.港股.%s(%s)" % (name, tk))

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
# 数据完整性汇总（用于阻断「假数据绿灯交付」）
# ----------------------------------------------------------------------
def build_integrity(boards: dict) -> dict:
    """汇总代理使用情况与核心口径降级情况。"""
    proxy_items = sorted(set(_PROXY_TRACE))
    core_degraded = []

    b1q = boards["board_1_macro_assets_7d"]["data_quality"]
    b2q = boards["board_2_regions_7d"]["data_quality"]
    b3q = boards["board_3_sectors_7d"]["data_quality"]

    # 核心口径：必须是真实资金流（不能是代理、不能是 failed）
    core = [
        ("board1.A股", b1q.get("A股")),
        ("board1.港股", b1q.get("港股")),
        ("board1.加密货币", b1q.get("加密货币")),
        ("board2.CN", b2q.get("CN")),
        ("board2.HK", b2q.get("HK")),
    ]
    for name, q in core:
        if q and q != "ok":
            core_degraded.append("%s=%s" % (name, q))

    failed_sectors = [k for k, v in (b3q.get("A股") or {}).items() if v != "ok"]
    if failed_sectors:
        core_degraded.append("board3.A股板块失败: " + ", ".join(failed_sectors))

    return {
        "all_real": len(proxy_items) == 0 and len(core_degraded) == 0,
        "proxy_items": proxy_items,
        "degraded": len(core_degraded) > 0,
        "core_failures": core_degraded,
        "notes": _NOTES,
    }


# ----------------------------------------------------------------------
# 主程序
# ----------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="fund_flow_data.json")
    parser.add_argument("--date", default=None,
                        help="覆盖数据日期（用于离线自检）")
    parser.add_argument("--allow-degraded", action="store_true",
                        help="允许核心口径降级时仍产出（默认拒绝并退出非零）")
    parser.add_argument("--strict-proxy", action="store_true",
                        help="连动能代理也不允许（要求全量真实资金流）")
    args = parser.parse_args()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log.info("=== 开始采集三层看板数据 (%s UTC) ===", now)

    boards = {
        "board_1_macro_assets_7d": board1_macro(),
        "board_2_regions_7d": board2_regions(),
        "board_3_sectors_7d": board3_sectors(),
    }

    integrity = build_integrity(boards)

    payload = {
        "schema_version": "3.0",
        "update_time_utc": now,
        "data_date": args.date,
        "fx_basis": {"USD": 1.0, "CNY": FX_TO_USD["CNY"], "HKD": FX_TO_USD["HKD"]},
        "disclaimer": (
            "value_usd 为按固定汇率折算后的美元金额；"
            "标记为 is_proxy=true 的条目为量价动能代理（A/D 动能），"
            "非真实资金净流入，前端必须显著区分展示。"
        ),
        "data_integrity": integrity,
    }
    payload.update(boards)

    # ---- 硬门槛：核心口径降级时拒绝产出 ----
    if integrity["degraded"] and not args.allow_degraded:
        log.error("=" * 70)
        log.error("核心口径数据不达标，拒绝产出 fund_flow_data.json")
        for item in integrity["core_failures"]:
            log.error("   - %s", item)
        for n in integrity["notes"]:
            log.error("   note: %s", n)
        log.error("=" * 70)
        log.error("如确需在降级状态下产出（仅调试用），请加 --allow-degraded")
        raise SystemExit(1)

    if args.strict_proxy and integrity["proxy_items"]:
        log.error("--strict-proxy 已开启，但存在动能代理条目: %s",
                  integrity["proxy_items"])
        raise SystemExit(1)

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    log.info("已写入 %s", args.out)
    if integrity["proxy_items"]:
        log.warning("本次存在 %d 处动能代理（is_proxy=true），前端须显著区分: %s",
                    len(integrity["proxy_items"]), integrity["proxy_items"])
    for n in integrity["notes"]:
        log.warning("note: %s", n)


if __name__ == "__main__":
    main()
