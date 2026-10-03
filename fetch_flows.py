# -*- coding: utf-8 -*-
"""
全球资本流动监测终端 · 后端采集引擎
================================================

本版修订（v4）解决的问题：

  A. 【接口名不存在】原脚本调用了两个 akshare 中根本不存在的函数：
       - ak.stock_market_fund_flow_hist  -> 从未存在，A股备线路是死代码
       - ak.stock_hk_ggt_historical      -> 从未存在，港股线路是死代码
     本版删除死代码，改用真实存在的 stock_hsgt_hist_em(symbol=...) 取南向资金。

  B. 【东财 push2/push2his 被 WAF 拦截】实测结论（重要）：
       - 裸 TCP + TLS 握手全部成功（28~29ms），说明域名与网络层正常；
       - GET / 能正常返回 404，说明 HTTP 协议栈正常；
       - GET /api/qt/* 返回「空响应」，服务端收到请求后直接 RST —— 属 WAF 接口级拦截；
       - 加完整浏览器头、复用 Session、拉到 5 秒间隔、强制固定 IP、裸 socket
         手写 HTTP —— 全部失败；免费代理池实测 0/15 可用。
     结论：东财 push2/push2his 在 CI（GitHub Runner 的 Azure IP）环境下不可用。
           本版不再依赖它为主线路。

  C. 【A股板块改用同花顺】ak.stock_fund_flow_industry(symbol=...)
       - 数据源 data.10jqka.com.cn，完全不经过东财，实测稳定可用；
       - 一次请求返回 90 个行业的「流入资金 / 流出资金 / 净额 / 涨跌幅 / 公司家数」；
       - 支持 5 个统计窗口：即时 / 3日排行 / 5日排行 / 10日排行 / 20日排行；
       - 注意1：即时窗口用列名「行业-涨跌幅」，排行窗口用「阶段涨跌幅」，需兼容；
       - 注意2：【无日期字段】数据本身不含交易日信息，必须由外部锚定（见 D）。

  D. 【日期锚定】同花顺「即时」= 最近一个交易日的日度数据（非此刻实时）。
     实测：2026-10-03 周六 20:39（非交易日）仍返回 90 行完整数据，
           与南向资金接口最新日期 2026-09-30 一致。
     因此必须推断数据归属的交易日，否则休市日会重复采集同一天的数据。
     本版实现三级锚定：南向资金真实日期 > 交易日历推断 > 采集日。

  E. 【降级不得静默产出（沿用 v3）】
       - 每条记录强制携带 metric_type（口径说明）+ is_proxy（是否动能代理）；
       - 核心口径不达标时直接 SystemExit(1)，让 CI 红着退出。
         宁可不产出，也不产出「看着正常的假数据」。

  F. 沿用已修好的部分：
       - ASHR 汇率 bug（美股挂牌 ETF 以美元计价，不再除以 7.1）
       - 全局请求节流 _throttle() + 指数退避重试
       - attach_intensity 真递归
       - 废弃 datetime.utcnow()

输出契约（schema v4.0）：
    {
      "schema_version": "4.0",
      "update_time_utc": "...",
      "data_date": "2026-09-30",
      "data_date_source": "anchored_by_hsgt",
      "fx_basis": {...},
      "disclaimer": "...",
      "data_integrity": {
          "all_real": true/false,
          "proxy_items": [...],
          "degraded": true/false,
          "core_failures": [...],
          "notes": [...]
      },
      "board_1_macro_assets_7d": {...},
      "board_2_regions_7d": {...},
      "board_3_sectors_7d": {...}
    }

运行：
  python3 fetch_flows.py --out fund_flow_data.json
  python3 fetch_flows.py --out fund_flow_data.json --allow-degraded   # 调试用
  python3 fetch_flows.py --out fund_flow_data.json --windows 即时,5日排行,20日排行
"""

import argparse
import json
import logging
import time
from datetime import datetime, timedelta, timezone

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

# 北京时间（A股/港股数据归属以此为准）
SH_TZ = timezone(timedelta(hours=8))

# ----------------------------------------------------------------------
# 汇率：集中一处，便于按需改为实时汇率
# ----------------------------------------------------------------------
FX_TO_USD = {
    "CNY": 7.10,   # 1 USD = 7.10 CNY
    "HKD": 7.80,   # 1 USD = 7.80 HKD
    "USD": 1.00,
}

# ------------------------------------------------------------------
# 【修复 · 量纲统一】数量级换算表
# ------------------------------------------------------------------
# 背景：同花顺 / 南向资金接口返回的原始值单位是「亿元」，而脚本原先把它
#       当作「元」直接除汇率，导致 value_usd / net_usd 的实际单位是「亿美元」，
#       与美股 A/D 代理的「美元」口径相差 1e8 倍 —— 跨资产无法同轴比较。
#
# 方案：native_unit 支持 "<币种>_<数量级>" 语法（如 CNY_100M 表示「亿元人民币」），
#       换算时先乘数量级还原到基准货币单位，再除汇率。
#       目标：所有对外输出的 value_usd / net_usd 统一为【美元】。
UNIT_SCALE = {
    "BASE": 1.0,      # 基准货币单位（元 / 美元 / 港元）
    "100M": 1e8,      # 「亿」= 1e8
    "1K": 1e3,        # 「千」
    "1M": 1e6,        # 「百万」
    "1B": 1e9,        # 「十亿」
}

RETRY = 4                 # 重试次数（含首次）
RETRY_SLEEP = 2.0         # 指数退避基数（秒）
REQUEST_INTERVAL = 1.2    # 相邻外网请求最小间隔（秒），降低被限流概率
_last_request_ts = 0.0

# 同花顺行业资金流的默认统计窗口（可被 --windows 覆盖）
DEFAULT_WINDOWS = ["即时", "5日排行", "20日排行"]
VALID_WINDOWS = {"即时", "3日排行", "5日排行", "10日排行", "20日排行"}

# 全局采集痕迹
_PROXY_TRACE: list = []
_NOTES: list = []


def _mark_proxy(where: str) -> None:
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
    典型：东财 push2/push2his 的 WAF 接口级拦截（TCP/TLS 正常，HTTP 层被 RST）。
    """
    msg = str(exc).lower()
    return (
        "remote end closed connection" in msg
        or "connection aborted" in msg
        or "remotedisconnected" in msg
        or "connection reset" in msg
    )


def fx(native_unit: str) -> float:
    """返回 native -> USD 的除数（仅汇率部分，不含数量级）。"""
    return FX_TO_USD.get(_split_unit(native_unit)[0], 1.0)


def _split_unit(native_unit: str) -> tuple:
    """
    拆分 "<币种>[_<数量级>]" → (币种, 数量级系数)。
    兼容历史调用：未带数量级后缀时按基准单位处理。

        "CNY"        -> ("CNY", 1.0)
        "CNY_100M"   -> ("CNY", 1e8)     # 亿元人民币
        "USD"        -> ("USD", 1.0)
    """
    raw = (native_unit or "USD").upper()
    if "_" in raw:
        cur, scale = raw.split("_", 1)
        return cur, UNIT_SCALE.get(scale, 1.0)
    return raw, UNIT_SCALE["BASE"]


def to_usd(value: float, native_unit: str) -> float:
    """
    native 单位数值 -> USD（统一为【美元】）。

    换算 = value × 数量级系数 ÷ 汇率
      to_usd(14.97,  "CNY_100M") -> 14.97亿 CNY ≈ 2.1085e8 USD
      to_usd(40.74,  "CNY_100M") -> 40.74亿 CNY ≈ 5.7380e8 USD
      to_usd(1.23e7, "USD")      -> 1.23e7 USD
    """
    _, scale = _split_unit(native_unit)
    return value * scale / fx(native_unit)


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
# 日期锚定：同花顺数据本身不含日期，必须由外部确定归属交易日
# ----------------------------------------------------------------------
def infer_trade_date_by_calendar(now_sh: datetime) -> str:
    """
    兜底方案：按交易日历推断「最近一个已收盘的交易日」。
    规则：
      - 交易日 15:00 后 -> 当日
      - 交易日 15:00 前 -> 上一工作日
      - 周末           -> 最近一个周五
    局限：无法处理法定节假日（长假期间会指向错误的日期），
          因此仅作为最终兜底，优先使用南向资金接口的真实日期锚定。
    """
    d = now_sh.date()
    # 交易日 15:00 前，算作上一交易日
    if d.weekday() < 5 and now_sh.hour < 15:
        d -= timedelta(days=1)
    # 回退到工作日
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


def resolve_trade_date() -> tuple:
    """
    确定本次采集数据归属的交易日。
    返回 (date_str, source)，source ∈ {"anchored_by_hsgt", "inferred_by_calendar"}。

    优先用南向资金接口的真实「日期」字段锚定 —— 该接口返回真实交易日，
    可正确处理国庆等长假（实测：2026-10-03 周六返回最新日期 2026-09-30）。
    """
    if _ak_has("stock_hsgt_hist_em"):
        try:
            _throttle()
            sb = ak.stock_hsgt_hist_em(symbol="南向资金")
            if sb is not None and not sb.empty:
                latest = str(sb["日期"].iloc[-1])
                log.info("日期锚定：南向资金最新交易日 = %s", latest)
                return latest, "anchored_by_hsgt"
        except Exception as exc:  # noqa: BLE001
            log.warning("日期锚定失败（南向资金）: %s", exc)

    inferred = infer_trade_date_by_calendar(datetime.now(SH_TZ))
    log.warning("日期锚定：回退到交易日历推断 = %s（长假期间可能不准）", inferred)
    _mark_note(
        "数据日期由交易日历推断（%s），未能用南向资金锚定 —— "
        "若处于长假期间，该日期可能不准确。" % inferred
    )
    return inferred, "inferred_by_calendar"


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
                time.sleep(RETRY_SLEEP * (2 ** attempt) + 3.0)
                continue
        time.sleep(RETRY_SLEEP * attempt)
    log.error("[%s] 重试 %d 次后仍失败", ticker, RETRY)
    return pd.DataFrame()


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
# A股：同花顺行业资金流（主线路，不依赖东财）
# ----------------------------------------------------------------------
def _pick(cols, *names, default=None):
    """在列名候选中取第一个存在的列名。"""
    for n in names:
        if n in cols:
            return n
    return default


def fetch_cn_sectors_ths(windows=None):
    """
    A股行业资金流（同花顺，data.10jqka.com.cn）。

    返回 (payload_dict, data_quality)。data_quality ∈ ok/degraded/failed。

    payload_dict 结构：
      {
        "as_of": "2026-09-30",          # 数据归属交易日（由外部锚定）
        "source": "同花顺 data.10jqka.com.cn",
        "unit": "亿元",
        "industry_count": 90,
        "windows": {
          "即时":    [ {行业, 流入资金, 流出资金, 净额, 涨跌幅, 公司家数}, ...90 ],
          "5日排行": [ ... ],
          "20日排行":[ ... ]
        }
      }

    注意：
      1. 「即时」口径是「最近一个交易日的日度数据」，非此刻实时；
      2. 该接口【无日期字段】，as_of 由 resolve_trade_date() 外部锚定；
      3. 「即时」用列名「行业-涨跌幅」，排行窗口用「阶段涨跌幅」，需兼容。
    """
    windows = windows or DEFAULT_WINDOWS
    if not _ak_has("stock_fund_flow_industry"):
        log.error("akshare 缺少 stock_fund_flow_industry，A股行业资金流不可用")
        return None, "failed"

    valid = [w for w in windows if w in VALID_WINDOWS]
    if not valid:
        log.error("--windows 取值非法: %s", windows)
        return None, "failed"

    out_windows = {}
    for w in valid:
        try:
            _throttle()
            df = ak.stock_fund_flow_industry(symbol=w)
            if df is None or df.empty:
                log.warning("同花顺行业资金流 [%s] 返回空", w)
                continue

            cols = list(df.columns)
            # 「即时」用「行业-涨跌幅」；排行窗口用「阶段涨跌幅」
            pct_col = _pick(cols, "行业-涨跌幅", "阶段涨跌幅", "涨跌幅")

            rows = []
            for _, r in df.iterrows():
                industry = str(r.get("行业", "")).strip()
                if not industry:
                    continue
                net = pd.to_numeric(r.get("净额"), errors="coerce")
                inflow = pd.to_numeric(r.get("流入资金"), errors="coerce")
                outflow = pd.to_numeric(r.get("流出资金"), errors="coerce")
                if pd.isna(net):
                    continue
                pct = pd.to_numeric(r.get(pct_col), errors="coerce") if pct_col else None
                count = pd.to_numeric(r.get("公司家数"), errors="coerce")
                rows.append({
                    "industry": industry,
                    "inflow_100m": round(float(inflow), 4) if not pd.isna(inflow) else None,
                    "outflow_100m": round(float(outflow), 4) if not pd.isna(outflow) else None,
                    "net_100m": round(float(net), 4),
                    # 【修复】net 单位是「亿元」，须走 CNY_100M 数量级，
                    #        否则结果单位会是「亿美元」而非「美元」，与 value_usd 相差 1e8 倍
                    "net_usd": round(to_usd(float(net), "CNY_100M"), 2),
                    "pct_change": round(float(pct), 2) if (pct is not None and not pd.isna(pct)) else None,
                    "company_count": int(count) if not pd.isna(count) else None,
                })

            if rows:
                out_windows[w] = rows
                log.info("同花顺行业资金流 [%s]：%d 个行业", w, len(rows))
        except Exception as exc:  # noqa: BLE001
            if _is_connection_killed(exc):
                log.error("同花顺 [%s] 被连接层掐断: %s", w, exc)
            else:
                log.warning("同花顺 [%s] 失败: %s", w, exc)

    if not out_windows:
        _mark_note("同花顺行业资金流不可用（data.10jqka.com.cn）")
        return None, "failed"

    payload = {
        "source": "同花顺 data.10jqka.com.cn",
        "unit": "亿元",
        "windows": out_windows,
        "industry_count": len(next(iter(out_windows.values()))),
    }
    quality = "ok" if len(out_windows) == len(valid) else "degraded"
    return payload, quality


def fetch_cn_main_flow(days: int = 7):
    """
    A股大盘资金流（东财口径，作为可选思路保留）。
    东财 push2/push2his 在 CI 环境被 WAF 拦截（实测结论见模块 docstring B 段），
    因此本函数失败是预期行为，不再降级到动能代理。
    返回 (records, data_quality)。
    """
    if not _ak_has("stock_market_fund_flow"):
        log.info("akshare 缺少 stock_market_fund_flow，跳过东财 A股大盘线路")
        return [], "failed"

    try:
        _throttle()
        df = ak.stock_market_fund_flow()
        if df is not None and not df.empty:
            out = []
            for _, r in df.tail(days).iterrows():
                raw = float(r["主力净流入-净额"])
                out.append(rec(
                    date=str(r["日期"]),
                    value_native=raw / 1e8,
                    # 【修复】raw 原单位为元，此处已 /1e8 转为「亿元」 → CNY_100M
                    native_unit="CNY_100M",
                    metric_type="A股主力净流入（东财口径，单位已转亿）",
                    is_proxy=False,
                    value_cny_100m=round(raw / 1e8, 2),
                ))
            log.info("A股大盘主力净额（东财）：成功 %d 条", len(out))
            return out, "ok"
    except Exception as exc:  # noqa: BLE001
        if _is_connection_killed(exc):
            log.warning(
                "东财 A股大盘线路被 WAF 拦截（push2his，CI 环境预期如此）: %s", exc
            )
            _mark_note(
                "东财 push2/push2his 被 WAF 接口级拦截（TCP/TLS 正常，HTTP 层被 RST），"
                "CI 环境不可用；A股数据改由同花顺提供。"
            )
        else:
            log.warning("东财 A股大盘线路失败: %s", exc)

    return [], "failed"


# ----------------------------------------------------------------------
# 港股南向资金（真实接口：stock_hsgt_hist_em，走 datacenter-web，实测可用）
# 单位：亿元（人民币）。合法 symbol: 南向资金/港股通沪/港股通深
# 注意：北向资金（沪股通/深股通）自 2024 年起官方停止披露，返回全 NaN，不可用。
# ----------------------------------------------------------------------
def fetch_hk_southbound_total(days: int = 7):
    """南向资金「合计」口径（推荐）。返回 (records, data_quality)。"""
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
                    # 【修复】「当日成交净买额」单位是亿元 → CNY_100M
                    native_unit="CNY_100M",
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
                    metric_type="稳定币总市值日变化（!= 净法币流入）",
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
# ------------------------------------------------------------------
# 【修复 · 强度字段兼容】金额字段候选
# ------------------------------------------------------------------
# 背景：时序记录用 value_usd，而 A股行业截面记录用 net_usd。
#       原实现只认 value_usd，导致 A股 270 条记录取不到值 →
#       max_abs 恒为 0 → intensity_score 恒为 0.0，强度完全失效。
# 修复：统一走 _read_metric()，按优先级读取存在的数值字段。
METRIC_VALUE_FIELDS = ("value_usd", "net_usd", "net_100m")


def _read_metric(item: dict) -> float:
    """从记录中读取金额数值，兼容不同数据源的字段命名。"""
    for key in METRIC_VALUE_FIELDS:
        if key in item:
            val = item.get(key)
            if isinstance(val, (int, float)):
                return float(val)
    return 0.0


def attach_intensity(node):
    """
    真递归：对任意嵌套字典中的 list 节点，按其金额绝对值最大值
    归一化出 intensity_score（-100 ~ 100）。
    注意：分母为该窗口内最大值，故为「相对强度」，跨组不可比。
    """
    if isinstance(node, dict):
        for key, val in node.items():
            node[key] = attach_intensity(val)
        return node

    if isinstance(node, list):
        vals = [abs(_read_metric(f)) for f in node if isinstance(f, dict)]
        max_abs = max(vals) if vals else 0.0
        for f in node:
            if isinstance(f, dict):
                raw = _read_metric(f)
                f["intensity_score"] = (
                    round(raw / max_abs * 100, 1) if max_abs else 0.0
                )
        return node

    return node


# ----------------------------------------------------------------------
# 看板 1：宏观大类资产
# ----------------------------------------------------------------------
def board1_macro(data_date: str, windows=None) -> dict:
    log.info(">>> 看板1：宏观大类资产")
    data, quality = {}, {}

    # A股：同花顺行业资金流（主口径，截面）
    sectors, q = fetch_cn_sectors_ths(windows)
    if sectors:
        sectors["as_of"] = data_date
        sectors["as_of_source"] = "resolve_trade_date()"
        data["A股"] = sectors
    else:
        data["A股"] = None
    quality["A股"] = q

    # 港股：真实南向资金（时间序列，走 datacenter-web）
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
def board2_regions(data_date: str, windows=None) -> dict:
    log.info(">>> 看板2：区域市场")
    data, quality = {}, {}

    us = ad_momentum_proxy(yf_history("SPY"), "SPY")
    data["US"], quality["US"] = us, "ok" if us else "failed"
    if us:
        _mark_proxy("board2.US(SPY)")

    # CN：东财大盘（可能被 WAF 拦）+ 同花顺行业截面（主）
    cn_flow, q_cn = fetch_cn_main_flow()
    cn_sectors, q_sectors = fetch_cn_sectors_ths(windows)
    if cn_sectors:
        cn_sectors["as_of"] = data_date
    data["CN"] = {
        "main_flow": cn_flow if cn_flow else None,
        "sectors_snapshot": cn_sectors,
    }
    # CN 的质量以「至少有一个可用」为准
    quality["CN"] = "ok" if (cn_flow or cn_sectors) else "failed"

    data["HK"], quality["HK"] = fetch_hk_southbound_total()

    return {
        "status": "success",
        "data": attach_intensity(data),
        "data_quality": quality,
    }


# ----------------------------------------------------------------------
# 看板 3：股市细分板块
# ----------------------------------------------------------------------
def board3_sectors(data_date: str, windows=None) -> dict:
    log.info(">>> 看板3：股市细分板块")
    data, quality = {}, {}

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

    # A股板块：同花顺 90 行业截面（替代原 fetch_a_sector 的 5 个板块）
    cn_sectors, q_cn = fetch_cn_sectors_ths(windows)
    if cn_sectors:
        cn_sectors["as_of"] = data_date
        data["A股"] = cn_sectors
    else:
        data["A股"] = None

    # 港股板块 ETF：港股本土标的（.HK 后缀），
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
        "美股": {k: ("ok" if v else "failed") for k, v in data["美股"].items()},
        "A股": q_cn,
        "港股": {k: ("ok" if v else "failed") for k, v in data["港股"].items()},
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
        ("board3.A股", b3q.get("A股")),
    ]
    for name, q in core:
        if q and q != "ok":
            core_degraded.append("%s=%s" % (name, q))

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
                        help="覆盖数据归属交易日（默认自动锚定）")
    parser.add_argument("--windows", default=",".join(DEFAULT_WINDOWS),
                        help="同花顺统计窗口，逗号分隔。可选: %s"
                             % ",".join(sorted(VALID_WINDOWS)))
    parser.add_argument("--allow-degraded", action="store_true",
                        help="允许核心口径降级时仍产出（默认拒绝并退出非零）")
    parser.add_argument("--strict-proxy", action="store_true",
                        help="连动能代理也不允许（要求全量真实资金流）")
    args = parser.parse_args()

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log.info("=== 开始采集三层看板数据 (%s UTC) ===", now_utc)

    windows = [w.strip() for w in args.windows.split(",") if w.strip()]

    # 日期锚定
    if args.date:
        data_date, date_source = args.date, "cli_override"
    else:
        data_date, date_source = resolve_trade_date()

    boards = {
        "board_1_macro_assets_7d": board1_macro(data_date, windows),
        "board_2_regions_7d": board2_regions(data_date, windows),
        "board_3_sectors_7d": board3_sectors(data_date, windows),
    }

    integrity = build_integrity(boards)

    payload = {
        "schema_version": "4.0",
        "update_time_utc": now_utc,
        "data_date": data_date,
        "data_date_source": date_source,
        "fx_basis": {"USD": 1.0, "CNY": FX_TO_USD["CNY"], "HKD": FX_TO_USD["HKD"]},
        "disclaimer": (
            "value_usd 为按固定汇率折算后的美元金额；"
            "【单位统一】所有对外输出的 value_usd / net_usd 单位一律为【美元】"
            "（原始值为「亿元」的接口已通过 CNY_100M 数量级换算还原），"
            "跨资产可直接同轴比较。"
            "标记为 is_proxy=true 的条目为量价动能代理（A/D 动能），"
            "非真实资金净流入，前端必须显著区分展示。"
            "intensity_score 为各 list 组内相对强度（分母=组内最大值），跨组不可比。"
            "A股行业资金流来自同花顺「即时」口径 —— 其含义为"
            "「最近一个交易日的日度数据」，非此刻实时；"
            "该接口本身不含日期字段，data_date 由 resolve_trade_date() 外部锚定。"
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

    log.info("已写入 %s（数据归属交易日: %s / %s）",
             args.out, data_date, date_source)
    if integrity["proxy_items"]:
        log.warning("本次存在 %d 处动能代理（is_proxy=true），前端须显著区分: %s",
                    len(integrity["proxy_items"]), integrity["proxy_items"])
    for n in integrity["notes"]:
        log.warning("note: %s", n)


if __name__ == "__main__":
    main()
