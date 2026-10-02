"""跨市场资金行为采集：首次抓取 5 年，后续抓取最近 30 天。

口径：
1. 北向/南向资金为真实跨境净买入，单位为亿元人民币。
2. ETF 免费行情不含完整的历史申购赎回净流量，因此统一保存成交额、
   方向性成交额代理、OBV 和 MFI(14)；代理指标不能等同真实净流入。
3. 美股行情走 yfinance；A股/港股 ETF 行情走 akshare（东财/新浪源），
   避免单一数据源失效导致整条流水线静默停更。

产出：
- cross_market_data/*.csv     原始行情与资金流（既有格式保持不变）
- cross_market_data/macro_data.json
  看板直接消费的结构化数据（三市场资金流 + 周期涨跌 + 日期轴）
- cross_market_data/state.json
"""

from __future__ import annotations

import datetime as dt
import json
import math
import sys
import time
from pathlib import Path

import akshare as ak
import pandas as pd
import requests
import yfinance as yf


SCHEMA_VERSION = 4  # 3 -> 4：新增 macro_data.json 产出 + akshare 数据源 + 陈旧校验
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

# ---------------------------------------------------------------------------
# 看板 JSON 输出参数
# ---------------------------------------------------------------------------
WINDOW_30D_TRADING_DAYS = 30   # 30 日窗口

# 数据陈旧阈值：任一市场最新交易日距今超过该天数即告警（自然日）
STALE_DAYS_WARNING = 5         # 超过 5 天 → 告警但不算失败
STALE_DAYS_ERROR = 15          # 超过 15 天 → 判定失败，让 workflow 变红

# 各市场最新数据不得早于该天数（用于强校验；仅对仍在更新的市场生效）
REQUIRED_FRESHNESS_DAYS = 15

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

# code -> (name, market, role, yahoo_symbol, currency)
# 注意：A股/港股定价源已切到 akshare，此处的 yahoo_symbol 仅作备用回退。
CN_HK_ETFS = {
    "510300": ("沪深300ETF", "A股", "大盘核心", "510300.SS", "CNY"),
    "510500": ("中证500ETF", "A股", "中盘成长", "510500.SS", "CNY"),
    "512890": ("红利低波ETF", "A股", "低利率避险", "512890.SS", "CNY"),
    "512000": ("券商ETF", "A股", "金融先锋", "512000.SS", "CNY"),
    "159995": ("芯片ETF", "A股", "TMT硬科技", "159995.SZ", "CNY"),
    "512010": ("医药ETF", "A股", "医药防御", "512010.SS", "CNY"),
    "516160": ("新能源ETF", "A股", "绿色制造", "516160.SS", "CNY"),
    "159928": ("主要消费ETF", "A股", "消费复苏", "159928.SZ", "CNY"),
    "513050": ("恒生科技ETF", "港股", "港股成长", "513050.SS", "CNY"),
    "510900": ("H股ETF", "港股", "国企价值", "510900.SS", "CNY"),
    "513970": ("恒生红利低波ETF", "港股", "高股息避险", "513970.SS", "CNY"),
    "513040": ("恒生互联网ETF", "港股", "互联网赛道", "513040.SS", "CNY"),
    "2800.HK": ("盈富基金", "港股", "离岸大盘基石", "2800.HK", "HKD"),
}

# 港股代码在 akshare 中的映射：code -> ak_symbol（不带后缀）
HK_AK_SYMBOL = {
    "2800.HK": "02800",
}

# A股代码在 akshare 中的映射：code -> ak_symbol（6 位纯数字）
CN_AK_SYMBOL = {c: c for c in CN_HK_ETFS if CN_HK_ETFS[c][1] == "A股"}


# ---------------------------------------------------------------------------
# 状态与窗口
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
        return bool(state.get("initialized")) and state.get("schema_version") == SCHEMA_VERSION
    except (OSError, json.JSONDecodeError):
        return False


def five_years_ago(today: dt.date) -> dt.date:
    try:
        return today.replace(year=today.year - 5)
    except ValueError:
        return today.replace(year=today.year - 5, day=28)


def fetch_window(today: dt.date) -> tuple[str, dt.date, dt.date]:
    if is_initialized():
        return "30天增量", today - dt.timedelta(days=30), today
    return "5年全量", five_years_ago(today), today


# ---------------------------------------------------------------------------
# 行情获取：美股（yfinance）
# ---------------------------------------------------------------------------
def yahoo_download(symbols: list[str], start: dt.date, end: dt.date) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            frame = yf.download(
                symbols,
                start=start.isoformat(),
                end=(end + dt.timedelta(days=1)).isoformat(),
                auto_adjust=False,
                progress=False,
                threads=True,
            )
            if frame is not None and not frame.empty:
                return frame
            last_error = RuntimeError("Yahoo Finance 返回空数据")
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Yahoo Finance 未返回数据: {symbols}（{last_error}）")


def yahoo_series(raw: pd.DataFrame, field: str, symbol: str) -> pd.Series:
    if isinstance(raw.columns, pd.MultiIndex):
        return raw[field][symbol]
    return raw[field]


def fetch_us(start: dt.date, end: dt.date) -> pd.DataFrame:
    raw = yahoo_download(list(US_ETFS), start, end)
    rows, errors = [], []
    for ticker, (name, role) in US_ETFS.items():
        try:
            item = pd.DataFrame(
                {
                    "date": raw.index,
                    "open": yahoo_series(raw, "Open", ticker),
                    "high": yahoo_series(raw, "High", ticker),
                    "low": yahoo_series(raw, "Low", ticker),
                    "close": yahoo_series(raw, "Close", ticker),
                    "adj_close": yahoo_series(raw, "Adj Close", ticker),
                    "volume": yahoo_series(raw, "Volume", ticker),
                }
            ).dropna(subset=["close"])
            if item.empty:
                raise RuntimeError("空数据")
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
        raise RuntimeError("美股 ETF 获取不完整: " + "；".join(errors))
    return pd.concat(rows, ignore_index=True)


# ---------------------------------------------------------------------------
# 行情获取：A股/港股 ETF（akshare 主源 + yfinance 回退）
# ---------------------------------------------------------------------------
def _ak_rows_to_frame(df: pd.DataFrame) -> pd.DataFrame:
    """把 akshare 返回的中文列 DataFrame 规整为标准英文字段。"""
    column_map = {
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
    }
    frame = df.rename(columns=column_map)
    keep = ["date", "open", "high", "low", "close", "volume"]
    missing = [c for c in keep if c not in frame.columns]
    if missing:
        raise RuntimeError(f"akshare 返回缺少列: {missing}（实际列: {list(df.columns)}）")
    frame = frame[keep].copy()
    frame["date"] = pd.to_datetime(frame["date"])
    for col in ("open", "high", "low", "close", "volume"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(subset=["close"])
    if frame.empty:
        raise RuntimeError("akshare 返回空数据")
    # 免费源无复权净值列，用收盘价代替 adj_close（仅影响涨跌幅口径，不引入虚假数据）
    frame["adj_close"] = frame["close"]
    frame = frame.sort_values("date")
    return frame


def ak_fetch_cn(code: str, start: dt.date, end: dt.date) -> pd.DataFrame:
    df = ak.fund_etf_hist_em(
        symbol=CN_AK_SYMBOL.get(code, code),
        period="daily",
        start_date=start.strftime("%Y%m%d"),
        end_date=end.strftime("%Y%m%d"),
        adjust="qfq",
    )
    return _ak_rows_to_frame(df)


def ak_fetch_hk(code: str, start: dt.date, end: dt.date) -> pd.DataFrame:
    symbol = HK_AK_SYMBOL.get(code, code.replace(".HK", "").lstrip("0").zfill(5))
    df = ak.stock_hk_hist(
        symbol=symbol,
        period="daily",
        start_date=start.strftime("%Y%m%d"),
        end_date=end.strftime("%Y%m%d"),
        adjust="qfq",
    )
    return _ak_rows_to_frame(df)


def yf_fetch_one(yahoo_symbol: str, start: dt.date, end: dt.date) -> pd.DataFrame:
    raw = yahoo_download([yahoo_symbol], start, end)
    frame = pd.DataFrame(
        {
            "date": raw.index,
            "open": yahoo_series(raw, "Open", yahoo_symbol),
            "high": yahoo_series(raw, "High", yahoo_symbol),
            "low": yahoo_series(raw, "Low", yahoo_symbol),
            "close": yahoo_series(raw, "Close", yahoo_symbol),
            "adj_close": yahoo_series(raw, "Adj Close", yahoo_symbol),
            "volume": yahoo_series(raw, "Volume", yahoo_symbol),
        }
    ).dropna(subset=["close"])
    if frame.empty:
        raise RuntimeError("空数据")
    return frame.sort_values("date")


def fetch_cn_hk_etfs(start: dt.date, end: dt.date) -> pd.DataFrame:
    """逐个标的抓取：akshare 优先，失败回退 yfinance，全部失败才报错。

    逐标的降级（而非整批 raise）是为了避免单个 symbol 抽风拖垮全部 A/H 数据。
    """
    rows, errors = [], []
    for code, (name, market, role, yahoo_symbol, currency) in CN_HK_ETFS.items():
        item = None
        tried: list[str] = []
        fetchers = (
            (ak_fetch_hk if market == "港股" else ak_fetch_cn, "akshare"),
            (lambda c, s, e, _y=yahoo_symbol: yf_fetch_one(_y, s, e), "yfinance"),
        )
        for fn, label in fetchers:
            try:
                candidate = fn(code, start, end)
                tried.append(f"{label}=OK")
                if not candidate.empty:
                    item = candidate
                    break
            except Exception as exc:  # noqa: BLE001
                tried.append(f"{label}={str(exc)[:60]}")
                time.sleep(0.5)
        if item is None or item.empty:
            errors.append(f"{code}: " + " | ".join(tried))
            continue
        item = item.copy()
        item.insert(1, "code", code)
        item.insert(2, "yahoo_symbol", yahoo_symbol)
        item.insert(3, "name", name)
        item.insert(4, "role", role)
        item.insert(5, "market", market)
        item.insert(6, "currency", currency)
        item["turnover_est"] = item["close"] * item["volume"]
        rows.append(item)
    if errors:
        raise RuntimeError("A/H 股 ETF 获取不完整: " + "；".join(errors))
    return pd.concat(rows, ignore_index=True)


# ---------------------------------------------------------------------------
# 指标
# ---------------------------------------------------------------------------
def add_flow_indicators(frame: pd.DataFrame, code_column: str) -> pd.DataFrame:
    enriched = []
    for _, item in frame.groupby(code_column, sort=False):
        item = item.copy().sort_values("date")
        item = item.dropna(subset=["close"])
        if item.empty:
            continue
        close_delta = item["close"].diff()
        direction = close_delta.gt(0).astype(int) - close_delta.lt(0).astype(int)
        typical_price = (item["high"] + item["low"] + item["close"]) / 3
        raw_money_flow = typical_price * item["volume"]
        typical_delta = typical_price.diff()
        positive = raw_money_flow.where(typical_delta > 0, 0.0)
        negative = raw_money_flow.where(typical_delta < 0, 0.0)
        positive_14 = positive.rolling(14, min_periods=14).sum()
        negative_14 = negative.rolling(14, min_periods=14).sum()
        money_ratio = positive_14 / negative_14.replace(0, float("nan"))

        item["return_1d_pct"] = item["adj_close"].pct_change() * 100
        item["volume_change_pct"] = item["volume"].pct_change() * 100
        item["directional_turnover_proxy"] = item["turnover_est"] * direction
        item["obv"] = (item["volume"] * direction).cumsum()
        item["mfi_14"] = 100 - 100 / (1 + money_ratio)
        item.loc[(negative_14 == 0) & (positive_14 > 0), "mfi_14"] = 100.0
        item.loc[(positive_14 == 0) & (negative_14 > 0), "mfi_14"] = 0.0
        enriched.append(item)
    if not enriched:
        raise RuntimeError(f"{code_column} 无有效数据可用于计算指标")
    return pd.concat(enriched, ignore_index=True)


# ---------------------------------------------------------------------------
# 跨境资金
# ---------------------------------------------------------------------------
def fetch_cross_border(flow_name: str, start: dt.date, end: dt.date) -> pd.DataFrame:
    type_code = {"北向资金": "005", "南向资金": "006"}[flow_name]
    if (end - start).days <= 31:
        response = requests.get(
            "https://datacenter-web.eastmoney.com/api/data/v1/get",
            params={
                "sortColumns": "TRADE_DATE",
                "sortTypes": "-1",
                "pageSize": "64",
                "pageNumber": "1",
                "reportName": "RPT_MUTUAL_DEAL_HISTORY",
                "columns": "ALL",
                "source": "WEB",
                "client": "WEB",
                "filter": f'(MUTUAL_TYPE="{type_code}")',
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json().get("result") or {}
        frame = pd.DataFrame(payload.get("data") or []).rename(
            columns={
                "TRADE_DATE": "date",
                "NET_DEAL_AMT": "net_buy_100m_cny",
                "BUY_AMT": "buy_100m_cny",
                "SELL_AMT": "sell_100m_cny",
            }
        )
        if frame.empty:
            raise RuntimeError(f"东方财富未返回{flow_name}增量数据")
        for column in ("net_buy_100m_cny", "buy_100m_cny", "sell_100m_cny"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce") / 100
    else:
        frame = ak.stock_hsgt_hist_em(symbol=flow_name).rename(
            columns={
                "日期": "date",
                "当日成交净买额": "net_buy_100m_cny",
                "买入成交额": "buy_100m_cny",
                "卖出成交额": "sell_100m_cny",
            }
        )

    frame["date"] = pd.to_datetime(frame["date"])
    mask = frame["date"].dt.date.between(start, end)
    result = frame.loc[
        mask, ["date", "net_buy_100m_cny", "buy_100m_cny", "sell_100m_cny"]
    ].copy()
    result.insert(1, "flow_name", flow_name)
    return result


# ---------------------------------------------------------------------------
# 落盘
# ---------------------------------------------------------------------------
def merge_and_write(
    path: Path,
    new_data: pd.DataFrame,
    keys: list[str],
    indicator_code_column: str | None = None,
) -> pd.DataFrame:
    if path.exists() and path.stat().st_size > 0:
        old_data = pd.read_csv(path)
        merged = pd.concat([old_data, new_data], ignore_index=True)
    else:
        merged = new_data.copy()

    merged = merged.dropna(subset=["date"])
    merged["date"] = pd.to_datetime(merged["date"]).dt.strftime("%Y-%m-%d")
    for key in keys:
        if key != "date":
            merged[key] = merged[key].astype(str)
    # 清理历史遗留的残缺行（如 close/market 为空的脏数据）
    for required in ("close",):
        if required in merged.columns:
            merged = merged[merged[required].notna()]
    if "market" in merged.columns:
        merged = merged[merged["market"].notna() & (merged["market"] != "")]
    merged = merged.drop_duplicates(keys, keep="last")
    merged = merged.sort_values(keys)

    merged = merged[[column for column in new_data.columns if column in merged.columns]]
    if indicator_code_column:
        merged = add_flow_indicators(merged, indicator_code_column)

    temp_path = path.with_suffix(path.suffix + ".tmp")
    merged.to_csv(temp_path, index=False, encoding="utf-8-sig")
    temp_path.replace(path)
    return merged


def write_csv_atomic(path: Path, frame: pd.DataFrame) -> None:
    temp_path = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temp_path, index=False, encoding="utf-8-sig")
    temp_path.replace(path)


def write_json_atomic(path: Path, payload: dict) -> None:
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    temp_path.replace(path)


# ---------------------------------------------------------------------------
# 月线聚合
# ---------------------------------------------------------------------------
def build_market_monthly(frame: pd.DataFrame, code_column: str) -> pd.DataFrame:
    daily = frame.copy()
    daily["date"] = pd.to_datetime(daily["date"])
    daily["month"] = daily["date"].dt.to_period("M").astype(str)
    daily = daily.sort_values([code_column, "date"])

    identity_columns = [
        column
        for column in ("name", "role", "market", "currency", "yahoo_symbol")
        if column in daily.columns
    ]
    aggregations: dict[str, tuple[str, str]] = {
        "month_start": ("date", "min"),
        "month_end": ("date", "max"),
        "open": ("open", "first"),
        "high": ("high", "max"),
        "low": ("low", "min"),
        "close": ("close", "last"),
        "adj_close": ("adj_close", "last"),
        "volume": ("volume", "sum"),
        "turnover_est": ("turnover_est", "sum"),
        "directional_turnover_proxy": ("directional_turnover_proxy", "sum"),
        "month_end_obv": ("obv", "last"),
        "month_end_mfi_14": ("mfi_14", "last"),
        "trading_days": ("date", "count"),
    }
    for column in identity_columns:
        aggregations[column] = (column, "last")

    monthly = (
        daily.groupby([code_column, "month"], as_index=False)
        .agg(**aggregations)
        .sort_values([code_column, "month"])
    )
    monthly["monthly_return_pct"] = (
        monthly.groupby(code_column)["adj_close"].pct_change() * 100
    )
    monthly["turnover_change_pct"] = (
        monthly.groupby(code_column)["turnover_est"].pct_change() * 100
    )
    return monthly


def build_cross_border_monthly(frame: pd.DataFrame) -> pd.DataFrame:
    daily = frame.copy()
    daily["date"] = pd.to_datetime(daily["date"])
    daily["month"] = daily["date"].dt.to_period("M").astype(str)
    monthly = (
        daily.groupby(["flow_name", "month"], as_index=False)
        .agg(
            month_start=("date", "min"),
            month_end=("date", "max"),
            net_buy_100m_cny=(
                "net_buy_100m_cny", lambda values: values.sum(min_count=1)
            ),
            buy_100m_cny=("buy_100m_cny", lambda values: values.sum(min_count=1)),
            sell_100m_cny=("sell_100m_cny", lambda values: values.sum(min_count=1)),
            trading_days=("date", "count"),
            positive_days=("net_buy_100m_cny", lambda values: int((values > 0).sum())),
            negative_days=("net_buy_100m_cny", lambda values: int((values < 0).sum())),
        )
        .sort_values(["flow_name", "month"])
    )
    for window in (3, 6, 12):
        monthly[f"net_buy_rolling_{window}m_100m_cny"] = monthly.groupby(
            "flow_name"
        )["net_buy_100m_cny"].transform(
            lambda values: values.rolling(window, min_periods=1).sum()
        )
    return monthly


# ---------------------------------------------------------------------------
# 看板结构化 JSON
# ---------------------------------------------------------------------------
def _clean_number(value, digits: int = 4):
    """把数值规整为可 JSON 序列化的形式；NaN/Inf -> None。"""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return round(number, digits)


def _series_by_code(frame: pd.DataFrame, code_column: str, dates: list[str]) -> dict:
    """按日期轴对齐，输出 {code: [值...]} 的等长数组。"""
    work = frame.copy()
    work["date"] = pd.to_datetime(work["date"])
    work["_d"] = work["date"].dt.strftime("%Y-%m-%d")
    axis = pd.Index(dates)
    out: dict[str, list] = {}
    for code, item in work.groupby(code_column, sort=False):
        item = item.sort_values("date")
        lookup = dict(zip(item["_d"], item["directional_turnover_proxy"]))
        out[str(code)] = [_clean_number(lookup.get(d)) for d in axis]
    return out


def _change_since(frame: pd.DataFrame, code_column: str, since: dt.date) -> dict:
    """窗口内涨跌幅%（用 adj_close 首尾计算）。"""
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


def _month_list(frame: pd.DataFrame, months: int) -> list[str]:
    work = frame.copy()
    work["date"] = pd.to_datetime(work["date"])
    periods = sorted(work["date"].dt.to_period("M").unique())
    return [str(p) for p in periods[-months:]]


def _daily_date_list(frame: pd.DataFrame, days: int) -> list[str]:
    """取 frame 内最后 N 个自然日（按出现过的交易日）。

    注意：单一数据源可能比其它市场落后数十天。30 日窗口若只取某一个市场的
    日历，落后市场会与其完全无交集而导致整段序列被静默清空。因此 30 日窗口
    一律由 build_macro_json 用「多市场日历并集」构造，本函数只做单源兜底。
    """
    work = frame.copy()
    work["date"] = pd.to_datetime(work["date"])
    unique_days = sorted(work["date"].dt.strftime("%Y-%m-%d").unique())
    return unique_days[-days:]


def _union_daily_dates(frames: list[pd.DataFrame], days: int) -> list[str]:
    """多市场交易日并集，取最后 N 天。

    用并集（而非交集）可保证：某市场当天不开市/缺数据时该位置为 None，
    但不会因为自身日历落后就把整条序列判空。
    """
    all_days: set[str] = set()
    for frame in frames:
        if frame is None or frame.empty:
            continue
        work = frame.copy()
        work["date"] = pd.to_datetime(work["date"])
        all_days.update(work["date"].dt.strftime("%Y-%m-%d").unique())
    return sorted(all_days)[-days:]


def build_macro_json(
    todays: dt.date,
    us_all: pd.DataFrame,
    etf_all: pd.DataFrame,
    southbound_all: pd.DataFrame,
    northbound_all: pd.DataFrame,
    freshness: dict[str, str],
) -> dict:
    """生成看板直接消费的 macro_data.json。

    约定：
    - dates.30d / dates.5y 的粒度与该市场的流量数组严格对齐
    - market_flows.*: 方向性成交额代理（单位：原币，元/港元）
    - sector_chg.*:  窗口涨跌幅（%）
    """
    us = us_all.copy()
    etf = etf_all.copy()
    us["date"] = pd.to_datetime(us["date"])
    etf["date"] = pd.to_datetime(etf["date"])

    # ---- 日期轴 ----
    # 30 日窗口取「美股 + A/港 ETF」交易日并集：单一市场的日历可能整体落后
    # （如 yfinance 停更），用并集可避免该市场序列被静默清空。
    dates_30d = _union_daily_dates([us, etf], WINDOW_30D_TRADING_DAYS)
    dates_5y = _month_list(us, 60)

    # ---- 美股：30 日逐日 + 5 年月度 ----
    us_daily_30 = us
    us_monthly = build_market_monthly(us, "ticker")
    us_monthly_5y = us_monthly[us_monthly["month"].isin(dates_5y)]
    # 月度流量按日期轴对齐（月度轴用 month 字符串）
    us_flow_5y = {}
    for ticker, item in us_monthly_5y.groupby("ticker", sort=False):
        lookup = dict(zip(item["month"], item["directional_turnover_proxy"]))
        us_flow_5y[str(ticker)] = [_clean_number(lookup.get(m)) for m in dates_5y]

    # ---- A股 / 港股 ----
    cn = etf[etf["market"] == "A股"]
    hk = etf[etf["market"] == "港股"]
    cn_monthly = build_market_monthly(cn, "code") if not cn.empty else pd.DataFrame()
    hk_monthly = build_market_monthly(hk, "code") if not hk.empty else pd.DataFrame()

    def flow_30d(frame: pd.DataFrame, col: str) -> dict:
        if frame.empty:
            return {}
        # 不做 isin 预过滤：_series_by_code 会按轴逐日取值，缺失日自然为 None，
        # 预过滤反而会在日历不重叠时把整段序列清空。
        return _series_by_code(frame, col, dates_30d)

    def flow_month(
        monthly: pd.DataFrame, dates: list[str], col: str
    ) -> dict:
        if monthly.empty:
            return {}
        work = monthly[monthly["month"].isin(dates)]
        out = {}
        for code, item in work.groupby(col, sort=False):
            lookup = dict(zip(item["month"], item["directional_turnover_proxy"]))
            out[str(code)] = [_clean_number(lookup.get(m)) for m in dates]
        return out

    # ---- 南向资金（港股）月度 ----
    sb = southbound_all.copy()
    sb["date"] = pd.to_datetime(sb["date"])
    sb_monthly = build_cross_border_monthly(sb)
    sb_lookup = dict(
        zip(sb_monthly["month"], pd.to_numeric(sb_monthly["net_buy_100m_cny"], errors="coerce"))
    )
    south_5y = [_clean_number(sb_lookup.get(m)) for m in dates_5y]

    since_30d = todays - dt.timedelta(days=30)
    since_5y = five_years_ago(todays)

    # 注意：south_bound 单位是亿人民币，与 ETF 的成交额口径不同，单独标注
    payload = {
        "meta": {
            "updated_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "status": "ok",
            "source": (
                "美股 11 板块=yfinance 行情；A股/港股 ETF=akshare 行情；"
                "北向/南向=东方财富真实跨境净买入。资金流向为"
                "「成交额×涨跌方向」代理量，非真实申购赎回。"
            ),
            "coverage": {
                "US": int(us["ticker"].nunique()),
                "CN": int(cn["code"].nunique()) if not cn.empty else 0,
                "HK": int(hk["code"].nunique()) if not hk.empty else 0,
            },
            "freshness": freshness,
            "schema": "cross_market_data/v4",
        },
        "dates": {
            "30d": dates_30d,
            "5y": dates_5y,
        },
        "market_flows": {
            "us_sectors_30d": _series_by_code(us_daily_30, "ticker", dates_30d),
            "us_sectors_5y": us_flow_5y,
            "cn_sectors_30d": flow_30d(cn, "code"),
            "cn_sectors_5y": flow_month(cn_monthly, dates_5y, "code"),
            "hk_sectors_30d": flow_30d(hk, "code"),
            "hk_sectors_5y": flow_month(hk_monthly, dates_5y, "code"),
        },
        "sector_chg": {
            "us_30d": _change_since(us, "ticker", since_30d),
            "us_5y": _change_since(us, "ticker", since_5y),
            "cn_30d": _change_since(cn, "code", since_30d) if not cn.empty else {},
            "cn_5y": _change_since(cn, "code", since_5y) if not cn.empty else {},
            "hk_30d": _change_since(hk, "code", since_30d) if not hk.empty else {},
            "hk_5y": _change_since(hk, "code", since_5y) if not hk.empty else {},
        },
        "source_meta": {
            "us_flows": {
                "provider": "yfinance（Yahoo Finance）",
                "caliber": "日线收盘价 × 成交量，按涨跌方向带符号后累计",
                "unit": "美元",
                "latest_date": freshness.get("美股"),
            },
            "cn_etf": {
                "provider": "akshare fund_etf_hist_em（东方财富）",
                "caliber": "日线收盘价 × 成交量，按涨跌方向带符号后累计",
                "unit": "人民币元",
                "count": int(cn["code"].nunique()) if not cn.empty else 0,
                "latest_date": freshness.get("A股"),
            },
            "hk_etf": {
                "provider": "akshare stock_hk_hist（东方财富）",
                "caliber": "日线收盘价 × 成交量，按涨跌方向带符号后累计",
                "unit": "港元 / 人民币元（H股ETF 类为人民币计价）",
                "count": int(hk["code"].nunique()) if not hk.empty else 0,
                "latest_date": freshness.get("港股"),
            },
            "hk_southbound": {
                "provider": "东方财富 RPT_MUTUAL_DEAL_HISTORY",
                "caliber": "港股通真实成交净买额（沪港通 002 + 深港通 004 合并）",
                "unit": "亿元人民币",
                "latest_date": freshness.get("南向"),
            },
            "northbound": {
                "provider": "东方财富 RPT_MUTUAL_DEAL_HISTORY",
                "caliber": "陆股通真实成交净买额",
                "unit": "亿元人民币",
                "latest_date": freshness.get("北向"),
            },
        },
        "hk_southbound_5y": south_5y,
    }
    return payload


# ---------------------------------------------------------------------------
# 新鲜度校验
# ---------------------------------------------------------------------------
def check_freshness(frames: dict[str, pd.DataFrame], today: dt.date) -> tuple[dict[str, str], list[str]]:
    """返回 (各市场最新日期, 告警列表)。"""
    latest: dict[str, str] = {}
    warnings: list[str] = []
    for label, frame in frames.items():
        if frame is None or frame.empty:
            latest[label] = "无数据"
            warnings.append(f"[陈旧] {label} 没有任何数据")
            continue
        dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
        if dates.empty:
            latest[label] = "无数据"
            warnings.append(f"[陈旧] {label} 日期列无法解析")
            continue
        newest = dates.max().date()
        latest[label] = newest.isoformat()
        age = (today - newest).days
        if age > STALE_DAYS_ERROR:
            warnings.append(
                f"[失败] {label} 最新数据 {newest} 距今 {age} 天（>{STALE_DAYS_ERROR}），数据源疑似失效"
            )
        elif age > STALE_DAYS_WARNING:
            warnings.append(f"[告警] {label} 最新数据 {newest} 距今 {age} 天（>{STALE_DAYS_WARNING}）")
    return latest, warnings


# ---------------------------------------------------------------------------
# 汇总打印
# ---------------------------------------------------------------------------
def print_market_summary(title: str, frame: pd.DataFrame, code_column: str) -> None:
    print(f"\n=== {title} ===")
    enriched = add_flow_indicators(frame, code_column)
    for code, item in enriched.groupby(code_column, sort=False):
        item = item.sort_values("date")
        change = (item["adj_close"].iloc[-1] / item["adj_close"].iloc[0] - 1) * 100
        directional = item["directional_turnover_proxy"].sum() / 1e8
        latest_mfi = item["mfi_14"].dropna()
        mfi_text = f"{latest_mfi.iloc[-1]:.1f}" if not latest_mfi.empty else "N/A"
        print(
            f"{str(code):8s} {item['name'].iloc[-1]:12s} "
            f"涨跌 {change:+8.2f}%  "
            f"方向性成交额 {directional:+10.2f} 亿{item['currency'].iloc[-1]}  "
            f"MFI14 {mfi_text}"
        )


def print_cross_border_summary(title: str, frame: pd.DataFrame) -> None:
    values = pd.to_numeric(frame["net_buy_100m_cny"], errors="coerce")
    valid_dates = pd.to_datetime(frame.loc[values.notna(), "date"])
    coverage = (
        f"{valid_dates.min().date()} 至 {valid_dates.max().date()}"
        if not valid_dates.empty
        else "无有效净买入数据"
    )
    print(
        f"{title}累计净买入：{values.sum():+.2f} 亿元；"
        f"有效记录 {values.notna().sum()} 条；覆盖 {coverage}"
    )


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = dt.date.today()
    mode, start, end = fetch_window(today)
    print(f"开始执行 {mode}：{start} 至 {end}")

    us_new = fetch_us(start, end)
    etf_new = fetch_cn_hk_etfs(start, end)
    northbound_new = fetch_cross_border("北向资金", start, end)
    southbound_new = fetch_cross_border("南向资金", start, end)

    us_all = merge_and_write(US_FILE, us_new, ["date", "ticker"], "ticker")
    etf_all = merge_and_write(ETF_FILE, etf_new, ["date", "code"], "code")
    northbound_all = merge_and_write(
        NORTHBOUND_FILE, northbound_new, ["date", "flow_name"]
    )
    southbound_all = merge_and_write(
        SOUTHBOUND_FILE, southbound_new, ["date", "flow_name"]
    )

    us_monthly = build_market_monthly(us_all, "ticker")
    etf_monthly = build_market_monthly(etf_all, "code")
    northbound_monthly = build_cross_border_monthly(northbound_all)
    southbound_monthly = build_cross_border_monthly(southbound_all)
    write_csv_atomic(US_MONTHLY_FILE, us_monthly)
    write_csv_atomic(ETF_MONTHLY_FILE, etf_monthly)
    write_csv_atomic(NORTHBOUND_MONTHLY_FILE, northbound_monthly)
    write_csv_atomic(SOUTHBOUND_MONTHLY_FILE, southbound_monthly)

    # ---- 新鲜度校验（关键：让静默停更变得可见） ----
    freshness, warnings = check_freshness(
        {
            "美股": us_all,
            "A股": etf_all[etf_all["market"] == "A股"] if "market" in etf_all else etf_all,
            "港股": etf_all[etf_all["market"] == "港股"] if "market" in etf_all else etf_all,
            "北向": northbound_all,
            "南向": southbound_all,
        },
        today,
    )
    print("\n=== 数据新鲜度 ===")
    for label, date_text in freshness.items():
        print(f"  {label:4s} 最新数据：{date_text}")
    if warnings:
        print("\n=== 数据质量告警 ===")
        for line in warnings:
            print("  " + line)

    # ---- 生成看板结构化数据 ----
    macro_json = build_macro_json(
        today, us_all, etf_all, southbound_all, northbound_all, freshness
    )
    write_json_atomic(MACRO_JSON_FILE, macro_json)
    print(
        f"\n已生成看板数据：{MACRO_JSON_FILE.name} "
        f"（覆盖 美股 {macro_json['meta']['coverage']['US']} / "
        f"A股 {macro_json['meta']['coverage']['CN']} / "
        f"港股 {macro_json['meta']['coverage']['HK']}，"
        f"30日轴 {len(macro_json['dates']['30d'])} 点，"
        f"5年轴 {len(macro_json['dates']['5y'])} 点）"
    )

    STATE_FILE.write_text(
        json.dumps(
            {
                "initialized": True,
                "schema_version": SCHEMA_VERSION,
                "last_success_date": today.isoformat(),
                "last_mode": mode,
                "last_window_start": start.isoformat(),
                "last_window_end": end.isoformat(),
                "us_etf_count": len(US_ETFS),
                "a_h_etf_count": len(CN_HK_ETFS),
                "freshness": freshness,
                "warnings": warnings,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print_market_summary("美股 ETF 本次窗口资金行为", us_new, "ticker")
    print_market_summary("A/H 股 ETF 本次窗口资金行为", etf_new, "code")
    print("\n=== 真实跨境资金流 ===")
    print_cross_border_summary("北向资金", northbound_new)
    print_cross_border_summary("南向资金", southbound_new)
    print(f"\n历史数据目录：{DATA_DIR}")
    print(
        f"累计记录：美股 {len(us_all)}，A/H ETF {len(etf_all)}，"
        f"北向 {len(northbound_all)}，南向 {len(southbound_all)}"
    )
    print(
        f"月线记录：美股 {len(us_monthly)}，A/H ETF {len(etf_monthly)}，"
        f"北向 {len(northbound_monthly)}，南向 {len(southbound_monthly)}"
    )

    # ---- 致命陈旧：非零退出，让 GitHub Actions 变红 ----
    fatal = [w for w in warnings if w.startswith("[失败]")]
    if fatal:
        print("\n!!! 数据源疑似失效，任务失败 !!!", file=sys.stderr)
        for line in fatal:
            print("  " + line, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
