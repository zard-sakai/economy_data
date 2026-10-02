"""跨市场资金行为采集：首次抓取 5 年，后续抓取最近 30 天。

口径：
1. 北向/南向资金为真实跨境净买入，单位为亿元人民币。
2. ETF 免费行情不含完整的历史申购赎回净流量，因此统一保存成交额、
   方向性成交额代理、OBV 和 MFI(14)；代理指标不能等同真实净流入。
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import akshare as ak
import pandas as pd
import requests
import yfinance as yf


SCHEMA_VERSION = 3
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
    "516160": ("新能源ETF", "A股", "绿色制造", "516160.SS", "CNY"),
    "159928": ("主要消费ETF", "A股", "消费复苏", "159928.SZ", "CNY"),
    "513050": ("恒生科技ETF", "港股", "港股成长", "513050.SS", "CNY"),
    "510900": ("H股ETF", "港股", "国企价值", "510900.SS", "CNY"),
    "513970": ("恒生红利低波ETF", "港股", "高股息避险", "513970.SS", "CNY"),
    "513040": ("恒生互联网ETF", "港股", "互联网赛道", "513040.SS", "CNY"),
    "2800.HK": ("盈富基金", "港股", "离岸大盘基石", "2800.HK", "HKD"),
}


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


def yahoo_download(symbols: list[str], start: dt.date, end: dt.date) -> pd.DataFrame:
    frame = yf.download(
        symbols,
        start=start.isoformat(),
        end=(end + dt.timedelta(days=1)).isoformat(),
        auto_adjust=False,
        progress=False,
        threads=True,
    )
    if frame.empty:
        raise RuntimeError(f"Yahoo Finance 未返回数据: {symbols}")
    return frame


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
        except Exception as exc:
            errors.append(f"{ticker}: {exc}")
    if errors:
        raise RuntimeError("美股 ETF 获取不完整: " + "；".join(errors))
    return pd.concat(rows, ignore_index=True)


def fetch_cn_hk_etfs(start: dt.date, end: dt.date) -> pd.DataFrame:
    symbols = [spec[3] for spec in CN_HK_ETFS.values()]
    raw = yahoo_download(symbols, start, end)
    rows, errors = [], []
    for code, (name, market, role, yahoo_symbol, currency) in CN_HK_ETFS.items():
        try:
            item = pd.DataFrame(
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
            if item.empty:
                raise RuntimeError("空数据")
            item.insert(1, "code", code)
            item.insert(2, "yahoo_symbol", yahoo_symbol)
            item.insert(3, "name", name)
            item.insert(4, "role", role)
            item.insert(5, "market", market)
            item.insert(6, "currency", currency)
            item["turnover_est"] = item["close"] * item["volume"]
            rows.append(item)
        except Exception as exc:
            errors.append(f"{code}: {exc}")
    if errors:
        raise RuntimeError("A/H 股 ETF 获取不完整: " + "；".join(errors))
    return pd.concat(rows, ignore_index=True)


def add_flow_indicators(frame: pd.DataFrame, code_column: str) -> pd.DataFrame:
    enriched = []
    for _, item in frame.groupby(code_column, sort=False):
        item = item.copy().sort_values("date")
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
    return pd.concat(enriched, ignore_index=True)


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

    merged["date"] = pd.to_datetime(merged["date"]).dt.strftime("%Y-%m-%d")
    for key in keys:
        if key != "date":
            merged[key] = merged[key].astype(str)
    merged = merged.drop_duplicates(keys, keep="last")

    merged = merged[[column for column in new_data.columns if column in merged.columns]]
    if indicator_code_column:
        merged = add_flow_indicators(merged, indicator_code_column)
    merged = merged.sort_values(keys)

    temp_path = path.with_suffix(path.suffix + ".tmp")
    merged.to_csv(temp_path, index=False, encoding="utf-8-sig")
    temp_path.replace(path)
    return merged


def write_csv_atomic(path: Path, frame: pd.DataFrame) -> None:
    temp_path = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temp_path, index=False, encoding="utf-8-sig")
    temp_path.replace(path)


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
            f"{code:8s} {item['name'].iloc[-1]:12s} "
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


if __name__ == "__main__":
    main()
