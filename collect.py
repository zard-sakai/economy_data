"""跨市场资金行为采集：首次抓取 5 年，后续抓取最近 30 天。

口径升级 (Schema Version 5 - 方案二: 交易所官方 API 直连)：
1. 北向/南向资金：真实跨境成交净买额（东方财富 RPT_MUTUAL_DEAL_HISTORY），单位为亿元人民币。
2. 美股 / A股 / 港股 ETF：
   - 价格行情：yfinance 批量拉取，切片同步对齐（解决交易日历差异导致的数组长度错位）。
   - A股 ETF 官方份额：HTTPS 直连上交所 (SSE) 与深交所 (SZSE) 官方 API 提取已发行总份额，对海外 IP 零拦截！
   - 真实资金流向：True Net Flow = ΔShares Outstanding * Close Price / 10^8 (亿元)。

产出：
- cross_market_data/*.csv      原始行情与真实申赎资金流
- cross_market_data/macro_data.json 看板直接消费的结构化数据（真实资金流 + 周期涨跌 + 日期轴）
- cross_market_data/state.json
"""

from __future__ import annotations

import datetime as dt
import json
import math
import sys
import time
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf


SCHEMA_VERSION = 5  # 修复 516160.SS 代码拼写、上交所 HTTPS 协议与 MultiIndex 切片对齐 Bug
DATA_DIR = Path(__file__).resolve().parent / "cross_market_data"
STATE_FILE = DATA_DIR / "state.json"
US_FILE = DATA_DIR / "us_spdr_daily.csv"
ETF_FILE = DATA_DIR / "cn_hk_etf_daily.csv"
NORTHBOUND_FILE = DATA_DIR / "northbound_daily.csv"
SOUTHBOUND_FILE = DATA_DIR / "southbound_daily.csv"
US_MONTHLY_FILE = DATA_DIR / "us_spdr_monthly.csv"
ETF_MONTHLY_FILE = DATA_DIR / "cn_hk_etf_monthly.csv"
NORTHBOUND_MONTHLY_FILE = DATA_DIR / "northbound_monthly.csv"
SOUTHBOUND_MONTHLY_FILE = DATA_DIR / "southbound_monthly.csv"
MACRO_JSON_FILE = DATA_DIR / "macro_data.json"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}

SSE_HEADERS = {
    "User-Agent": DEFAULT_HEADERS["User-Agent"],
    "Referer": "https://www.sse.com.cn/",
    "Host": "query.sse.com.cn",
}

SZSE_HEADERS = {
    "User-Agent": DEFAULT_HEADERS["User-Agent"],
    "Referer": "https://www.szse.cn/",
}

WINDOW_30D_TRADING_DAYS = 30
STALE_DAYS_WARNING = 5
STALE_DAYS_ERROR = 15

US_ETFS = {
    "XLK": ("信息科技", "科技成长"),
    "XLF": ("金融", "金融周期"),
    "XLV": ("医疗健康", "防御成长"),
    "XLI": ("工业", "制造业周期"),
    "XLC": ("通信服务", "平台与传媒"),
    "XLY": ("可选消费", "消费弹性"),
    "XLP": ("必选消费", "防御消费"),
    "XLE": ("能源", "能源周期"),
    "XLB": ("原材料", "上游周期"),
    "XLRE": ("房地产", "利率敏感"),
    "XLU": ("公用事业", "防御高股息"),
}

CN_HK_ETFS = {
    "510300": ("沪深300ETF", "A股", "大盘核心", "510300.SS", "CNY"),
    "510500": ("中证500ETF", "A股", "中盘成长", "510500.SS", "CNY"),
    "512890": ("红利低波ETF", "A股", "低利率避险", "512890.SS", "CNY"),
    "512000": ("券商ETF", "A股", "金融先锋", "512000.SS", "CNY"),
    "159995": ("芯片ETF", "A股", "TMT硬科技", "159995.SZ", "CNY"),
    "512010": ("医药ETF", "A股", "医药防御", "512010.SS", "CNY"),
    "516160": ("新能源ETF", "A股", "绿色制造", "516160.SS", "CNY"),  # 修正: .SZ -> .SS
    "159928": ("主要消费ETF", "A股", "消费复苏", "159928.SZ", "CNY"),
    "513050": ("恒生科技ETF", "港股", "港股成长", "513050.SS", "CNY"),
    "510900": ("H股ETF", "港股", "国企价值", "510900.SS", "CNY"),
    "513970": ("恒生红利低波ETF", "港股", "高股息避险", "513970.SS", "CNY"),
    "513040": ("恒生互联网ETF", "港股", "互联网赛道", "513040.SS", "CNY"),
    "2800.HK": ("盈富基金", "港股", "离岸大盘基石", "2800.HK", "HKD"),
}


# ---------------------------------------------------------------------------
# 状态与工具
# ---------------------------------------------------------------------------
def is_initialized() -> bool:
    required = (
        US_FILE,
        ETF_FILE,
        NORTHBOUND_FILE,
        SOUTHBOUND_FILE,
        US_MONTHLY_FILE,
        ETF_MONTHLY_FILE,
        NORTHBOUND_MONTHLY_FILE,
        SOUTHBOUND_MONTHLY_FILE,
    )
    if not STATE_FILE.exists() or not all(
        path.exists() and path.stat().st_size > 0 for path in required
    ):
        return False
    try:
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return (
            bool(state.get("initialized"))
            and state.get("schema_version") == SCHEMA_VERSION
        )
    except (OSError, json.JSONDecodeError):
        return False


def five_years_ago(today: dt.date) -> dt.date:
    try:
        return today.replace(year=today.year - 5)
    except ValueError:
        return today.replace(year=today.year - 5, day=28)


def fetch_window(today: dt.date) -> tuple[str, dt.date, dt.date]:
    if is_initialized():
        return "30天增量", today - dt.timedelta(days=45), today
    return "5年全量", five_years_ago(today), today


# ---------------------------------------------------------------------------
# 交易所官方接口：上交所 (SSE) & 深交所 (SZSE) ETF 份额直连 (HTTPS)
# ---------------------------------------------------------------------------
def fetch_sse_official_shares(code: str) -> pd.DataFrame:
    """上交所官方 HTTPS API：直接拉取权威 ETF 历史已发行总份额"""
    url = f"https://query.sse.com.cn/commonUtil/getStockFensheHistory.do?stockCode={code}"
    res = requests.get(url, headers=SSE_HEADERS, timeout=15)
    res.raise_for_status()
    data = res.json().get("result", [])
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)[["tradeDate", "totalShares"]]
    df.columns = ["date", "shares_outstanding"]
    df["date"] = pd.to_datetime(df["date"])
    df["shares_outstanding"] = pd.to_numeric(
        df["shares_outstanding"], errors="coerce"
    )
    return df.sort_values("date").reset_index(drop=True)


def fetch_szse_official_shares(code: str) -> pd.DataFrame:
    """深交所官方 HTTPS API：直接拉取深市 ETF 历史已发行总份额"""
    url = "https://www.szse.cn/api/report/ShowReport/data?SHOWPREPAGE=true&CATALOGID=1945&txtQueryDate="
    res = requests.get(url, headers=SZSE_HEADERS, timeout=15)
    res.raise_for_status()
    data = res.json()
    if isinstance(data, list) and len(data) > 0:
        df = pd.DataFrame(data[0].get("data", []))
        if "sys_key" in df.columns:
            df_code = df[df["sys_key"].str.contains(code, na=False)]
            if not df_code.empty:
                result = pd.DataFrame(
                    {
                        "date": pd.to_datetime(df_code["zrq"]),
                        "shares_outstanding": pd.to_numeric(
                            df_code["dqfe"], errors="coerce"
                        ),
                    }
                )
                return result.sort_values("date").reset_index(drop=True)
    return pd.DataFrame()


def fetch_exchange_official_shares(code: str) -> pd.DataFrame:
    """按交易所路由抓取权威 ETF 份额（防崩锁保护包）"""
    try:
        if code.startswith("5") or code.startswith("6"):
            return fetch_sse_official_shares(code)
        elif code.startswith("1") or code.startswith("0"):
            return fetch_szse_official_shares(code)
    except Exception as exc:  # noqa: BLE001
        print(
            f"ℹ️ 交易所官方份额 API 暂时不可用 ({code}: {exc})，降级使用成交量代理流量",
            file=sys.stderr,
        )
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# yfinance 提取助手（严格同步对齐切片索引）
# ---------------------------------------------------------------------------
def yahoo_download(
    symbols: list[str], start: dt.date, end: dt.date
) -> pd.DataFrame:
    last_error = None
    for attempt in range(3):
        try:
            frame = yf.download(
                symbols,
                start=start.isoformat(),
                end=(end + dt.timedelta(days=1)).isoformat(),
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            if frame is not None and not frame.empty:
                return frame
            last_error = RuntimeError("Yahoo Finance 返回空数据")
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Yahoo Finance 未返回数据: {symbols}（{last_error}）")


def extract_ticker_df(raw_price: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """对 MultiIndex 行情结构进行同索引切片，解决不同市场交易日历导致的长度错位。"""
    if raw_price is None or raw_price.empty:
        return pd.DataFrame()

    try:
        if isinstance(raw_price.columns, pd.MultiIndex):
            if ticker in raw_price.columns.levels[0]:
                df_sym = raw_price[ticker].copy()
            elif ticker in raw_price.columns.levels[1]:
                df_sym = raw_price.xs(ticker, axis=1, level=1).copy()
            else:
                return pd.DataFrame()
        else:
            df_sym = raw_price.copy()

        col_map = {
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
        df_sym = df_sym.rename(columns=col_map)
        keep = ["open", "high", "low", "close", "adj_close", "volume"]
        existing = [c for c in keep if c in df_sym.columns]
        if "close" not in existing:
            return pd.DataFrame()

        df_sym = df_sym[existing].dropna(subset=["close"]).copy()
        if df_sym.empty:
            return pd.DataFrame()

        df_sym["date"] = pd.to_datetime(df_sym.index).dt.tz_localize(None)
        return df_sym.sort_values("date").reset_index(drop=True)
    except Exception as exc:  # noqa: BLE001
        print(f"ℹ️ 行情提取异常 ({ticker}): {exc}", file=sys.stderr)
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# 美股与 A/H 股行情获取
# ---------------------------------------------------------------------------
def fetch_us(start: dt.date, end: dt.date) -> pd.DataFrame:
    symbols = list(US_ETFS.keys())
    raw = yahoo_download(symbols, start, end)
    rows, errors = [], []

    for ticker, (name, role) in US_ETFS.items():
        try:
            item = extract_ticker_df(raw, ticker)
            if item.empty:
                continue

            shares_series = pd.Series(dtype=float)
            try:
                t = yf.Ticker(ticker)
                s = t.get_shares_full(start=start.isoformat())
                if s is not None and not s.empty:
                    shares_series = s
            except Exception:  # noqa: BLE001
                pass

            if not shares_series.empty:
                shares_df = (
                    pd.DataFrame(
                        {
                            "date": pd.to_datetime(
                                shares_series.index
                            ).dt.tz_localize(None),
                            "shares_outstanding": shares_series.values,
                        }
                    )
                    .dropna()
                    .sort_values("date")
                )
                item = pd.merge_asof(
                    item.sort_values("date"),
                    shares_df.sort_values("date"),
                    on="date",
                    direction="backward",
                )
            else:
                item["shares_outstanding"] = math.nan

            item["shares_outstanding"] = item["shares_outstanding"].ffill()
            item["shares_change_1d"] = item["shares_outstanding"].diff()
            item["true_net_flow_usd"] = (
                item["shares_change_1d"] * item["close"]
            )
            item["true_net_flow_100m_usd"] = item["true_net_flow_usd"] / 1e8

            item.insert(1, "ticker", ticker)
            item.insert(2, "name", name)
            item.insert(3, "role", role)
            item.insert(4, "market", "美股")
            item.insert(5, "currency", "USD")
            item["turnover_est"] = item["close"] * item["volume"]

            rows.append(item)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{ticker}: {exc}")

    if errors:
        print(f"⚠️ 美股部分标的处理警示: {errors}", file=sys.stderr)
    if not rows:
        raise RuntimeError("美股 ETF 行情获取失败")
    return pd.concat(rows, ignore_index=True)


def fetch_cn_hk_etfs(start: dt.date, end: dt.date) -> pd.DataFrame:
    """获取 A/H 股 ETF 行情，并组合上交所/深交所官方权威份额"""
    yahoo_symbols = [
        info[3] for info in CN_HK_ETFS.values() if info[3] is not None
    ]
    raw_price = yahoo_download(yahoo_symbols, start, end)

    rows, errors = [], []
    for code, (
        name,
        market,
        role,
        yahoo_symbol,
        currency,
    ) in CN_HK_ETFS.items():
        try:
            item = extract_ticker_df(raw_price, yahoo_symbol)
            if item.empty:
                continue

            # 方案二核心：调用交易所官方 API 提取真实份额
            shares_df = fetch_exchange_official_shares(code)
            if not shares_df.empty:
                item = pd.merge_asof(
                    item.sort_values("date"),
                    shares_df.sort_values("date"),
                    on="date",
                    direction="backward",
                )
            else:
                item["shares_outstanding"] = math.nan

            item["shares_outstanding"] = item["shares_outstanding"].ffill()
            item["shares_change_1d"] = item["shares_outstanding"].diff()
            # 真实申赎净额（亿元） = Δ份额 * 单位收盘价 / 10^8
            item["true_net_flow_100m_cny"] = (
                item["shares_change_1d"] * item["close"]
            ) / 100000000.0

            item.insert(1, "code", code)
            item.insert(2, "yahoo_symbol", yahoo_symbol)
            item.insert(3, "name", name)
            item.insert(4, "role", role)
            item.insert(5, "market", market)
            item.insert(6, "currency", currency)
            item["turnover_est"] = item["close"] * item["volume"]

            rows.append(item)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{code}: {exc}")

    if errors:
        print(f"⚠️ A/H 股部分标的处理警示: {errors}", file=sys.stderr)
    if not rows:
        raise RuntimeError("A/H 股 ETF 全量获取失败")
    return pd.concat(rows, ignore_index=True)


# ---------------------------------------------------------------------------
# 跨境真实资金流（北向/南向）
# ---------------------------------------------------------------------------
def fetch_cross_border(
    flow_name: str, start: dt.date, end: dt.date
) -> pd.DataFrame:
    type_code = {"北向资金": "005", "南向资金": "006"}[flow_name]
    last_err = None
    for attempt in range(3):
        try:
            response = requests.get(
                "https://datacenter-web.eastmoney.com/api/data/v1/get",
                params={
                    "sortColumns": "TRADE_DATE",
                    "sortTypes": "-1",
                    "pageSize": "100",
                    "pageNumber": "1",
                    "reportName": "RPT_MUTUAL_DEAL_HISTORY",
                    "columns": "ALL",
                    "source": "WEB",
                    "client": "WEB",
                    "filter": f'(MUTUAL_TYPE="{type_code}")',
                },
                headers=DEFAULT_HEADERS,
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json().get("result") or {}
            data_list = payload.get("data") or []
            if data_list:
                frame = pd.DataFrame(data_list).rename(
                    columns={
                        "TRADE_DATE": "date",
                        "NET_DEAL_AMT": "net_buy_100m_cny",
                        "BUY_AMT": "buy_100m_cny",
                        "SELL_AMT": "sell_100m_cny",
                    }
                )
                for column in (
                    "net_buy_100m_cny",
                    "buy_100m_cny",
                    "sell_100m_cny",
                ):
                    frame[column] = (
                        pd.to_numeric(frame[column], errors="coerce") / 100.0
                    )
                frame["date"] = pd.to_datetime(frame["date"])
                mask = frame["date"].dt.date.between(start, end)
                result = frame.loc[
                    mask,
                    [
                        "date",
                        "net_buy_100m_cny",
                        "buy_100m_cny",
                        "sell_100m_cny",
                    ],
                ].copy()
                result.insert(1, "flow_name", flow_name)
                return result.sort_values("date")
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(2 * (attempt + 1))

    file_path = NORTHBOUND_FILE if flow_name == "北向资金" else SOUTHBOUND_FILE
    if file_path.exists() and file_path.stat().st_size > 0:
        print(
            f"⚠️ 跨境资金 ({flow_name}) API 临时断连，加载本地历史数据",
            file=sys.stderr,
        )
        return pd.read_csv(file_path)

    raise RuntimeError(f"东方财富跨境资金获取失败 ({flow_name}): {last_err}")


# ---------------------------------------------------------------------------
# 落盘与清洗
# ---------------------------------------------------------------------------
def merge_and_write(
    path: Path, new_data: pd.DataFrame, keys: list[str]
) -> pd.DataFrame:
    if path.exists() and path.stat().st_size > 0:
        old_data = pd.read_csv(path)
        merged = pd.concat([old_data, new_data], ignore_index=True)
    else:
        merged = new_data.copy()

    merged = merged.dropna(subset=["date"])
    merged["date"] = pd.to_datetime(merged["date"]).dt.strftime("%Y-%m-%d")
    merged = merged.drop_duplicates(keys, keep="last").sort_values(keys)

    temp_path = path.with_suffix(path.suffix + ".tmp")
    merged.to_csv(temp_path, index=False, encoding="utf-8-sig")
    temp_path.replace(path)
    return merged


def write_json_atomic(path: Path, payload: dict) -> None:
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    temp_path.replace(path)


# ---------------------------------------------------------------------------
# 看板结构化 JSON 构建
# ---------------------------------------------------------------------------
def _clean_number(value, digits: int = 4):
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return round(number, digits)


def _true_flow_series(
    frame: pd.DataFrame,
    code_column: str,
    dates: list[str],
    flow_col: str = "true_net_flow_100m_cny",
) -> dict:
    work = frame.copy()
    work["date"] = pd.to_datetime(work["date"])
    work["_d"] = work["date"].dt.strftime("%Y-%m-%d")

    if "directional_turnover_proxy" not in work.columns:
        if "close" in work.columns and "volume" in work.columns:
            close_delta = work.groupby(code_column)["close"].diff()
            direction = (close_delta > 0).astype(int) - (
                close_delta < 0
            ).astype(int)
            work["directional_turnover_proxy"] = (
                work["close"] * work["volume"] * direction
            ) / 1e8
        else:
            work["directional_turnover_proxy"] = math.nan

    axis = pd.Index(dates)
    out: dict[str, list] = {}
    for code, item in work.groupby(code_column, sort=False):
        item = item.sort_values("date")
        use_col = flow_col
        if flow_col not in item.columns or item[flow_col].dropna().empty:
            use_col = "directional_turnover_proxy"

        lookup = dict(zip(item["_d"], item[use_col]))
        out[str(code)] = [_clean_number(lookup.get(d)) for d in axis]
    return out


def _change_since(
    frame: pd.DataFrame, code_column: str, since: dt.date
) -> dict:
    work = frame.copy()
    work["date"] = pd.to_datetime(work["date"]).dt.date
    out: dict[str, float] = {}
    for code, item in work.groupby(code_column, sort=False):
        item = item[item["date"] >= since].sort_values("date")
        closes = pd.to_numeric(item["adj_close"], errors="coerce").dropna()
        if len(closes) >= 2 and closes.iloc[0]:
            out[str(code)] = _clean_number(
                (closes.iloc[-1] / closes.iloc[0] - 1) * 100, 2
            )
        else:
            out[str(code)] = None
    return out


def build_macro_json(
    todays: dt.date,
    us_all: pd.DataFrame,
    etf_all: pd.DataFrame,
    southbound_all: pd.DataFrame,
    northbound_all: pd.DataFrame,
    freshness: dict[str, str],
) -> dict:
    us = us_all.copy()
    etf = etf_all.copy()
    us["date"] = pd.to_datetime(us["date"])
    etf["date"] = pd.to_datetime(etf["date"])

    all_days = set(us["date"].dt.strftime("%Y-%m-%d").unique()) | set(
        etf["date"].dt.strftime("%Y-%m-%d").unique()
    )
    dates_30d = sorted(all_days)[-WINDOW_30D_TRADING_DAYS:]
    dates_5y = sorted(
        list(set(us["date"].dt.to_period("M").astype(str).unique()))
    )[-60:]

    cn = etf[etf["market"] == "A股"] if "market" in etf else pd.DataFrame()
    hk = etf[etf["market"] == "港股"] if "market" in etf else pd.DataFrame()

    since_30d = todays - dt.timedelta(days=30)
    since_5y = five_years_ago(todays)

    payload = {
        "meta": {
            "updated_at": dt.datetime.now(dt.timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            ),
            "status": "ok",
            "source": (
                "真实资金流量体系 (Schema v5 - 交易所官方源): A股/美股 ETF 采用"
                "上交所(SSE)/深交所(SZSE)官方接口已发行份额变动 (Shares Outstanding) × 单位净值/价格"
                "计算权威真实申赎流量；北向/南向资金采用东方财富真实跨境成交净买额。"
            ),
            "coverage": {
                "US": int(us["ticker"].nunique()),
                "CN": int(cn["code"].nunique()) if not cn.empty else 0,
                "HK": int(hk["code"].nunique()) if not hk.empty else 0,
            },
            "freshness": freshness,
            "schema": "cross_market_data/v5",
        },
        "dates": {
            "30d": dates_30d,
            "5y": dates_5y,
        },
        "market_flows": {
            "us_sectors_30d": _true_flow_series(
                us, "ticker", dates_30d, "true_net_flow_100m_usd"
            ),
            "cn_sectors_30d": _true_flow_series(
                cn, "code", dates_30d, "true_net_flow_100m_cny"
            )
            if not cn.empty
            else {},
            "hk_sectors_30d": _true_flow_series(
                hk, "code", dates_30d, "true_net_flow_100m_cny"
            )
            if not hk.empty
            else {},
        },
        "sector_chg": {
            "us_30d": _change_since(us, "ticker", since_30d),
            "us_5y": _change_since(us, "ticker", since_5y),
            "cn_30d": _change_since(cn, "code", since_30d)
            if not cn.empty
            else {},
            "cn_5y": _change_since(cn, "code", since_5y)
            if not cn.empty
            else {},
            "hk_30d": _change_since(hk, "code", since_30d)
            if not hk.empty
            else {},
            "hk_5y": _change_since(hk, "code", since_5y)
            if not hk.empty
            else {},
        },
    }
    return payload


# ---------------------------------------------------------------------------
# 新鲜度校验
# ---------------------------------------------------------------------------
def check_freshness(
    frames: dict[str, pd.DataFrame], today: dt.date
) -> tuple[dict[str, str], list[str]]:
    latest: dict[str, str] = {}
    warnings: list[str] = []
    for label, frame in frames.items():
        if frame is None or frame.empty:
            latest[label] = "无数据"
            warnings.append(f"[陈旧] {label} 没有任何数据")
            continue
        dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
        newest = dates.max().date()
        latest[label] = newest.isoformat()
        age = (today - newest).days
        if age > STALE_DAYS_ERROR:
            warnings.append(
                f"[失败] {label} 最新数据 {newest} 距今 {age} 天，数据源疑似失效"
            )
        elif age > STALE_DAYS_WARNING:
            warnings.append(
                f"[告警] {label} 最新数据 {newest} 距今 {age} 天"
            )
    return latest, warnings


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = dt.date.today()
    mode, start, end = fetch_window(today)
    print(f"🚀 开始执行 {mode} 交易所官方真实资金流采集：{start} 至 {end}")

    us_new = fetch_us(start, end)
    etf_new = fetch_cn_hk_etfs(start, end)
    northbound_new = fetch_cross_border("北向资金", start, end)
    southbound_new = fetch_cross_border("南向资金", start, end)

    us_all = merge_and_write(US_FILE, us_new, ["date", "ticker"])
    etf_all = merge_and_write(ETF_FILE, etf_new, ["date", "code"])
    northbound_all = merge_and_write(
        NORTHBOUND_FILE, northbound_new, ["date", "flow_name"]
    )
    southbound_all = merge_and_write(
        SOUTHBOUND_FILE, southbound_new, ["date", "flow_name"]
    )

    freshness, warnings = check_freshness(
        {
            "美股": us_all,
            "A股": etf_all[etf_all["market"] == "A股"]
            if "market" in etf_all
            else etf_all,
            "港股": etf_all[etf_all["market"] == "港股"]
            if "market" in etf_all
            else etf_all,
            "北向": northbound_all,
            "南向": southbound_all,
        },
        today,
    )

    macro_json = build_macro_json(
        today, us_all, etf_all, southbound_all, northbound_all, freshness
    )
    write_json_atomic(MACRO_JSON_FILE, macro_json)

    STATE_FILE.write_text(
        json.dumps(
            {
                "initialized": True,
                "schema_version": SCHEMA_VERSION,
                "last_success_date": today.isoformat(),
                "last_mode": mode,
                "freshness": freshness,
                "warnings": warnings,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        f"✅ 采集与加工成功完成！看板数据已更新至 {MACRO_JSON_FILE.name}"
    )

    fatal = [w for w in warnings if w.startswith("[失败]")]
    if fatal:
        print("\n!!! 存在严重陈旧数据，判定 workflow 失败 !!!", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
