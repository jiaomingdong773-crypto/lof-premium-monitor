"""
LOF 基金实时溢价率监测 API (Vercel Python Serverless Function + 本地共用)
支持：
- 50 只热门套利 LOF（美股 QDII、原油商品、黄金贵金属、港股/海外、A股热门 LOF）
- 腾讯财经批量拉取场内实时价格与成交额（1 次请求）
- 东方财富 / 天天基金拉取最新官方净值、净值日期、A股实时估值、申购状态与单日限额
- 新浪财经批量拉取美股标的昨夜行情、盘中股指/商品期货、港股指数与离岸人民币汇率（1 次请求，零限流）
"""

from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler
from typing import Any

# 北京时间
CST = timezone(timedelta(hours=8))

# 50 只热门可能有套利机会的 LOF 基金配置表
FUNDS: list[dict[str, Any]] = [
    # ===== 1. 美股 QDII (10只) =====
    {"code": "161130", "name": "纳指LOF", "full_name": "易方达纳斯达克100LOF", "category": "美股QDII", "type": "qdii_us", "benchmark": "gb_qqq", "futures": "hf_NQ", "position": 0.94, "bench_name": "纳斯达克100 (QQQ + NQ期货)"},
    {"code": "161125", "name": "标普500LOF", "full_name": "易方达标普500LOF", "category": "美股QDII", "type": "qdii_us", "benchmark": "gb_spy", "futures": "hf_ES", "position": 0.94, "bench_name": "标普500 (SPY + ES期货)"},
    {"code": "161128", "name": "标普信息科技", "full_name": "易方达标普信息科技LOF", "category": "美股QDII", "type": "qdii_us", "benchmark": "gb_xlk", "futures": "hf_NQ", "position": 0.93, "bench_name": "标普科技 (XLK + NQ期货)"},
    {"code": "161127", "name": "标普生物科技", "full_name": "易方达标普生物科技LOF", "category": "美股QDII", "type": "qdii_us", "benchmark": "gb_xbi", "futures": "hf_ES", "position": 0.92, "bench_name": "标普生科 (XBI + ES期货)"},
    {"code": "161126", "name": "标普医疗保健", "full_name": "易方达标普医疗保健LOF", "category": "美股QDII", "type": "qdii_us", "benchmark": "gb_xlv", "futures": "hf_ES", "position": 0.92, "bench_name": "标普医疗 (XLV + ES期货)"},
    {"code": "162415", "name": "美国消费LOF", "full_name": "华宝标普美国消费LOF", "category": "美股QDII", "type": "qdii_us", "benchmark": "gb_xly", "futures": "hf_ES", "position": 0.92, "bench_name": "美国可选消费 (XLY + ES期货)"},
    {"code": "501225", "name": "全球芯片LOF", "full_name": "景顺长城全球半导体芯片LOF", "category": "美股QDII", "type": "qdii_us", "benchmark": "gb_soxx", "futures": "hf_NQ", "position": 0.90, "bench_name": "全球半导体 (SOXX + NQ期货)"},
    {"code": "501312", "name": "海外科技LOF", "full_name": "华宝海外科技LOF", "category": "美股QDII", "type": "qdii_us", "benchmark": "gb_arkk", "futures": "hf_NQ", "position": 0.90, "bench_name": "海外科技 (ARKK + NQ期货)"},
    {"code": "160140", "name": "美国REIT", "full_name": "南方道琼斯美国精选REIT", "category": "美股QDII", "type": "qdii_us", "benchmark": "gb_vnq", "futures": "hf_ES", "position": 0.92, "bench_name": "美国REITs (VNQ + ES期货)"},
    {"code": "501300", "name": "美元债LOF", "full_name": "海富通全球收益美元债LOF", "category": "美股QDII", "type": "qdii_us", "benchmark": "gb_agg", "futures": None, "position": 0.95, "bench_name": "全球美元债 (AGG)"},

    # ===== 2. 原油与大宗商品 QDII (8只) =====
    {"code": "162411", "name": "华宝油气LOF", "full_name": "华宝标普油气上游股票LOF", "category": "原油商品QDII", "type": "qdii_us", "benchmark": "gb_xop", "futures": "hf_CL", "position": 0.93, "bench_name": "油气上游股票 (XOP + WTI期货)"},
    {"code": "160416", "name": "石油基金LOF", "full_name": "华安标普全球石油LOF", "category": "原油商品QDII", "type": "qdii_us", "benchmark": "gb_ixc", "futures": "hf_CL", "position": 0.93, "bench_name": "全球石油股票 (IXC + WTI期货)"},
    {"code": "162719", "name": "石油LOF", "full_name": "广发道琼斯石油指数LOF", "category": "原油商品QDII", "type": "qdii_us", "benchmark": "gb_ieo", "futures": "hf_CL", "position": 0.92, "bench_name": "美股油气开采 (IEO + WTI期货)"},
    {"code": "501018", "name": "南方原油LOF", "full_name": "南方原油QDII-FOF-LOF", "category": "原油商品QDII", "type": "qdii_commodity", "benchmark": "gb_uso", "futures": "hf_CL", "position": 0.90, "bench_name": "WTI原油 (USO + CL期货)"},
    {"code": "161129", "name": "原油LOF易方达", "full_name": "易方达原油QDII-FOF-LOF", "category": "原油商品QDII", "type": "qdii_commodity", "benchmark": "gb_uso", "futures": "hf_CL", "position": 0.90, "bench_name": "WTI原油 (USO + CL期货)"},
    {"code": "160723", "name": "嘉实原油LOF", "full_name": "嘉实原油QDII-FOF-LOF", "category": "原油商品QDII", "type": "qdii_commodity", "benchmark": "gb_uso", "futures": "hf_CL", "position": 0.90, "bench_name": "WTI原油 (USO + CL期货)"},
    {"code": "160216", "name": "国泰商品LOF", "full_name": "国泰大宗商品QDII-LOF", "category": "原油商品QDII", "type": "qdii_commodity", "benchmark": "gb_uso", "futures": "hf_CL", "position": 0.88, "bench_name": "能源大宗商品 (USO + CL期货)"},
    {"code": "163208", "name": "诺安油气能源", "full_name": "诺安油气能源QDII-FOF-LOF", "category": "原油商品QDII", "type": "qdii_us", "benchmark": "gb_xle", "futures": "hf_CL", "position": 0.90, "bench_name": "全球油气能源 (XLE + CL期货)"},

    # ===== 3. 黄金、白银与商品 (6只) =====
    {"code": "161226", "name": "国投白银LOF", "full_name": "国投瑞银白银期货LOF", "category": "黄金贵金属", "type": "qdii_commodity", "benchmark": "gb_slv", "futures": "hf_SI", "position": 0.95, "bench_name": "白银期货 (SLV + SI期货)"},
    {"code": "164701", "name": "汇添富黄金LOF", "full_name": "汇添富黄金及贵金属QDII-LOF", "category": "黄金贵金属", "type": "qdii_commodity", "benchmark": "gb_gld", "futures": "hf_GC", "position": 0.92, "bench_name": "现货黄金 (GLD + GC期货)"},
    {"code": "160719", "name": "嘉实黄金LOF", "full_name": "嘉实黄金QDII-LOF", "category": "黄金贵金属", "type": "qdii_commodity", "benchmark": "gb_gld", "futures": "hf_GC", "position": 0.92, "bench_name": "现货黄金 (GLD + GC期货)"},
    {"code": "161116", "name": "黄金主题LOF", "full_name": "易方达黄金主题QDII-LOF", "category": "黄金贵金属", "type": "qdii_commodity", "benchmark": "gb_gld", "futures": "hf_GC", "position": 0.90, "bench_name": "黄金主题 (GLD + GC期货)"},
    {"code": "165513", "name": "信诚商品LOF", "full_name": "中信保诚全球商品主题LOF", "category": "黄金贵金属", "type": "qdii_commodity", "benchmark": "gb_dbc", "futures": "hf_CL", "position": 0.88, "bench_name": "全球大宗商品 (DBC)"},
    {"code": "161815", "name": "银华抗通胀LOF", "full_name": "银华抗通胀主题QDII-LOF", "category": "黄金贵金属", "type": "qdii_commodity", "benchmark": "gb_dbc", "futures": "hf_GC", "position": 0.88, "bench_name": "全球抗通胀商品 (DBC)"},

    # ===== 4. 港股 / 印度 / 海外中概 QDII (10只) =====
    {"code": "164824", "name": "印度基金LOF", "full_name": "工银瑞信印度市场LOF", "category": "港股/海外QDII", "type": "qdii_us", "benchmark": "gb_inda", "futures": None, "position": 0.92, "bench_name": "印度市场 (INDA)"},
    {"code": "164906", "name": "中概互联LOF", "full_name": "交银中证海外中国互联网LOF", "category": "港股/海外QDII", "type": "qdii_us", "benchmark": "gb_kweb", "futures": None, "position": 0.93, "bench_name": "海外中国互联网 (KWEB)"},
    {"code": "160644", "name": "港美互联网LOF", "full_name": "鹏华香港美国互联网LOF", "category": "港股/海外QDII", "type": "qdii_us", "benchmark": "gb_kweb", "futures": None, "position": 0.90, "bench_name": "港美互联网 (KWEB)"},
    {"code": "160717", "name": "恒生国企LOF", "full_name": "嘉实恒生中国企业LOF", "category": "港股/海外QDII", "type": "qdii_hk", "benchmark": "rt_hkHSCEI", "futures": None, "position": 0.93, "bench_name": "恒生中国企业指数 (HSCEI)"},
    {"code": "160924", "name": "恒生指数LOF", "full_name": "大成恒生指数LOF", "category": "港股/海外QDII", "type": "qdii_hk", "benchmark": "rt_hkHSI", "futures": None, "position": 0.93, "bench_name": "恒生指数 (HSI)"},
    {"code": "161124", "name": "港股小盘LOF", "full_name": "易方达恒生综合小型股LOF", "category": "港股/海外QDII", "type": "qdii_hk", "benchmark": "rt_hkHSI", "futures": None, "position": 0.92, "bench_name": "恒生小型股 (HSI参考)"},
    {"code": "501301", "name": "香港大盘LOF", "full_name": "华宝恒生中国30LOF", "category": "港股/海外QDII", "type": "qdii_hk", "benchmark": "rt_hkHSCEI", "futures": None, "position": 0.93, "bench_name": "香港大盘 (HSCEI参考)"},
    {"code": "501302", "name": "南方恒指LOF", "full_name": "南方恒生ETF联接LOF", "category": "港股/海外QDII", "type": "qdii_hk", "benchmark": "rt_hkHSI", "futures": None, "position": 0.94, "bench_name": "恒生指数 (HSI)"},
    {"code": "501021", "name": "香港中小LOF", "full_name": "华宝香港上市中国中小盘LOF", "category": "港股/海外QDII", "type": "qdii_hk", "benchmark": "rt_hkHSI", "futures": None, "position": 0.92, "bench_name": "香港中小盘 (HSI参考)"},
    {"code": "501025", "name": "香港银行LOF", "full_name": "鹏华中证香港银行LOF", "category": "港股/海外QDII", "type": "qdii_hk", "benchmark": "rt_hkHSCEI", "futures": None, "position": 0.93, "bench_name": "香港银行 (HSCEI参考)"},

    # ===== 5. A 股热门行业/主题 LOF (16只，使用天天基金实时估值或官方净值) =====
    {"code": "161725", "name": "白酒基金LOF", "full_name": "招商中证白酒指数LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "中证白酒 (实时盘中估值)"},
    {"code": "161726", "name": "生物医药LOF", "full_name": "招商国证生物医药LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "国证生物医药 (实时盘中估值)"},
    {"code": "161028", "name": "新能源车LOF", "full_name": "富国中证新能源汽车LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "中证新能源车 (实时盘中估值)"},
    {"code": "161024", "name": "军工LOF", "full_name": "富国中证军工指数LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "中证军工 (实时盘中估值)"},
    {"code": "161631", "name": "人工智能LOF", "full_name": "融通人工智能指数LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "中证人工智能 (实时盘中估值)"},
    {"code": "161724", "name": "煤炭LOF", "full_name": "招商中证煤炭等权LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "中证煤炭 (实时盘中估值)"},
    {"code": "160620", "name": "资源LOF", "full_name": "鹏华中证A股资源产业LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "中证资源 (实时盘中估值)"},
    {"code": "160625", "name": "证保LOF", "full_name": "鹏华中证800证券保险LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "中证证保 (实时盘中估值)"},
    {"code": "160628", "name": "地产LOF", "full_name": "鹏华中证800地产LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "中证地产 (实时盘中估值)"},
    {"code": "501016", "name": "券商基金LOF", "full_name": "国泰中证申万证券LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "申万证券 (实时盘中估值)"},
    {"code": "501029", "name": "红利基金LOF", "full_name": "华宝标普A股红利机会LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "A股红利机会 (实时盘中估值)"},
    {"code": "501011", "name": "中药LOF", "full_name": "汇添富中证中药ETF联接LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "中证中药 (实时盘中估值)"},
    {"code": "501057", "name": "汇添富新能车", "full_name": "汇添富中证新能源汽车LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "中证新能车 (实时盘中估值)"},
    {"code": "163406", "name": "兴全合润LOF", "full_name": "兴全合润混合LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.90, "bench_name": "主动偏股 (实时盘中估值)"},
    {"code": "161005", "name": "富国天惠LOF", "full_name": "富国天惠成长混合LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.90, "bench_name": "主动偏股 (实时盘中估值)"},
    {"code": "161039", "name": "中证1000增强", "full_name": "富国中证1000指数增强LOF", "category": "A股热门LOF", "type": "a_share", "benchmark": None, "futures": None, "position": 0.95, "bench_name": "中证1000增强 (实时盘中估值)"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
CACHE_TTL = 30.0

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def _http_get(url: str, timeout: float = 6.0, encoding: str = "utf-8", extra_headers: dict[str, str] | None = None) -> str:
    headers = dict(HEADERS)
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return raw.decode(encoding, errors="ignore")


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        s = str(v).strip()
        if not s or s in ("--", "null", "None"):
            return None
        return float(s)
    except (ValueError, TypeError):
        return None


def _market_prefix(code: str) -> str:
    return "sh" if code.startswith(("5", "6")) else "sz"


def _parse_us_date_str(est_str: str, fallback_year: int) -> str:
    """将 'Nov 20 04:00PM EST' 解析为 '2026-11-20'"""
    m = re.match(r"([A-Za-z]{3})\s+(\d{1,2})", est_str.strip())
    if not m:
        return ""
    mon = MONTH_MAP.get(m.group(1))
    day = int(m.group(2))
    if not mon:
        return ""
    return f"{fallback_year:04d}-{mon:02d}-{day:02d}"


def fetch_tencent_quotes(codes: list[str]) -> dict[str, dict[str, Any]]:
    """批量从腾讯财经拉取 50 只 LOF 场内实时行情（1 次请求）"""
    symbols = [f"{_market_prefix(c)}{c}" for c in codes]
    url = "https://qt.gtimg.cn/q=" + ",".join(symbols)
    result: dict[str, dict[str, Any]] = {}
    try:
        text = _http_get(url, timeout=6.0, encoding="gbk")
        for line in text.strip().split(";"):
            line = line.strip()
            if not line or '="' not in line:
                continue
            m = re.search(r'v_(?:sh|sz)(\d{6})="([^"]*)"', line)
            if not m:
                continue
            code = m.group(1)
            parts = m.group(2).split("~")
            if len(parts) < 38:
                continue
            price = _safe_float(parts[3])
            prev_close = _safe_float(parts[4])
            if (price is None or price <= 0) and prev_close and prev_close > 0:
                price = prev_close
            change_pct = _safe_float(parts[32])
            turnover_wan = _safe_float(parts[37])
            result[code] = {
                "market_name": parts[1],
                "price": price,
                "prev_close": prev_close,
                "change_pct": change_pct,
                "turnover_wan": turnover_wan,
                "quote_time": parts[30] if len(parts) > 30 else "",
            }
    except Exception as e:
        print(f"[warn] fetch_tencent_quotes failed: {e}")
    return result


def fetch_sina_global_quotes(symbols: list[str]) -> dict[str, dict[str, Any]]:
    """
    批量从新浪财经拉取所有美股ETF昨夜行情、盘中股指/商品期货、港股指数与离岸人民币汇率（1次请求，极快且稳定）
    """
    if not symbols:
        return {}
    url = "https://hq.sinajs.cn/list=" + ",".join(symbols)
    res: dict[str, dict[str, Any]] = {}
    try:
        text = _http_get(
            url,
            timeout=6.0,
            encoding="gbk",
            extra_headers={"Referer": "https://finance.sina.com.cn"},
        )
        cur_year = datetime.now(CST).year
        for line in text.strip().split(";"):
            line = line.strip()
            if not line or '="' not in line:
                continue
            m = re.search(r'hq_str_([^=]+)="([^"]*)"', line)
            if not m:
                continue
            sym = m.group(1)
            raw = m.group(2)
            if not raw:
                continue
            parts = raw.split(",")

            if sym.startswith("gb_"):
                # 美股 ETF: parts[1]=最新收盘价, parts[2]=涨跌幅(%), parts[25]=美东收盘时间, parts[26]=昨收
                price = _safe_float(parts[1]) if len(parts) > 1 else None
                chg_pct = _safe_float(parts[2]) if len(parts) > 2 else None
                trade_date = ""
                if len(parts) > 25 and parts[25]:
                    trade_date = _parse_us_date_str(parts[25], cur_year)
                if not trade_date and len(parts) > 3 and parts[3]:
                    # 北京时间早上收盘，减1天近似为美东日期
                    try:
                        dt = datetime.strptime(parts[3][:10], "%Y-%m-%d") - timedelta(days=1)
                        trade_date = dt.strftime("%Y-%m-%d")
                    except Exception:
                        pass
                res[sym] = {
                    "symbol": sym,
                    "name": parts[0],
                    "price": price,
                    "change_pct": chg_pct,  # 百分比数值，例如 -5.67
                    "date": trade_date,
                }
            elif sym.startswith("hf_"):
                # 外盘期货: parts[0]=当前价, parts[7]=昨结算价, parts[12]=日期
                price = _safe_float(parts[0]) if len(parts) > 0 else None
                prev_settle = _safe_float(parts[7]) if len(parts) > 7 else None
                chg_pct = None
                if price is not None and prev_settle and prev_settle > 0:
                    chg_pct = ((price / prev_settle) - 1.0) * 100.0
                res[sym] = {
                    "symbol": sym,
                    "name": parts[13] if len(parts) > 13 else sym,
                    "price": price,
                    "prev_close": prev_settle,
                    "change_pct": chg_pct,
                    "date": parts[12] if len(parts) > 12 else "",
                    "time": parts[6] if len(parts) > 6 else "",
                }
            elif sym.startswith("fx_"):
                # 离岸人民币: parts[1]=现价, parts[3]=昨收, parts[10]=涨跌幅(%), parts[17]=日期
                price = _safe_float(parts[1]) if len(parts) > 1 else None
                prev_close = _safe_float(parts[3]) if len(parts) > 3 else None
                chg_pct = None
                if price is not None and prev_close and prev_close > 0:
                    chg_pct = ((price / prev_close) - 1.0) * 100.0
                elif len(parts) > 10:
                    chg_pct = _safe_float(parts[10])
                res[sym] = {
                    "symbol": sym,
                    "name": parts[9] if len(parts) > 9 else sym,
                    "price": price,
                    "change_pct": chg_pct,
                    "date": parts[17] if len(parts) > 17 else "",
                }
            elif sym.startswith("rt_hk"):
                # 港股指数: parts[6]=现价, parts[3]=昨收, parts[8]=涨跌幅(%), parts[17]=日期(YYYY/MM/DD)
                price = _safe_float(parts[6]) if len(parts) > 6 else None
                chg_pct = _safe_float(parts[8]) if len(parts) > 8 else None
                d_str = (parts[17] if len(parts) > 17 else "").replace("/", "-")
                res[sym] = {
                    "symbol": sym,
                    "name": parts[1] if len(parts) > 1 else sym,
                    "price": price,
                    "change_pct": chg_pct,
                    "date": d_str,
                }
    except Exception as e:
        print(f"[warn] fetch_sina_global_quotes failed: {e}")
    return res


def fetch_tiantian_nav(code: str) -> dict[str, Any]:
    """从天天基金拉取官方最新净值、净值日期、A股盘中实时估值"""
    url = f"https://fundgz.1234567.com.cn/js/{code}.js?rt={int(time.time() * 1000)}"
    try:
        text = _http_get(url, timeout=4.5, encoding="utf-8", extra_headers={"Referer": "https://fund.eastmoney.com/"})
        m = re.search(r"jsonpgz\((\{.*\})\)", text)
        if m:
            obj = json.loads(m.group(1))
            return {
                "nav": _safe_float(obj.get("dwjz")),
                "nav_date": obj.get("jzrq") or "",
                "gsz": _safe_float(obj.get("gsz")),
                "gszzl": _safe_float(obj.get("gszzl")),
                "gztime": obj.get("gztime") or "",
            }
    except Exception:
        pass
    return {}


def fetch_eastmoney_fund_info(codes: list[str]) -> dict[str, dict[str, Any]]:
    """批量从东方财富移动端接口拉取最新官方净值、净值日期"""
    url = (
        "https://fundmobapi.eastmoney.com/FundMNewApi/FundMNFInfo"
        "?pageIndex=1&pageSize=100&plat=Android&appType=ttjj&product=EFund&Version=1"
        "&Fcodes=" + ",".join(codes)
    )
    res: dict[str, dict[str, Any]] = {}
    try:
        text = _http_get(url, timeout=6.0, encoding="utf-8", extra_headers={"Referer": "https://fund.eastmoney.com/"})
        obj = json.loads(text)
        for item in obj.get("Datas") or []:
            code = str(item.get("FCODE", ""))
            if not code:
                continue
            res[code] = {
                "nav": _safe_float(item.get("NAV")),
                "nav_date": item.get("PDATE") or "",
                "nav_chg": _safe_float(item.get("NAVCHGRT")),
                "short_name": item.get("SHORTNAME") or "",
            }
    except Exception as e:
        print(f"[warn] fetch_eastmoney_fund_info failed: {e}")
    return res


def fetch_eastmoney_purchase_status(code: str) -> dict[str, Any]:
    """从天天基金移动端接口拉取申购状态与单日限额"""
    url = (
        f"https://fundmobapi.eastmoney.com/FundMApi/FundBaseTypeInformation.ashx"
        f"?FCODE={code}&deviceid=Wap&plat=Wap&product=EFund&version=2.0.0"
    )
    try:
        text = _http_get(url, timeout=4.0, encoding="utf-8", extra_headers={"Referer": "https://fund.eastmoney.com/"})
        obj = json.loads(text)
        data = obj.get("Datas") or {}
        sgzt = (data.get("SGZT") or "").strip()
        max_sg = data.get("MAXSG")

        is_paused = "暂停申购" in sgzt
        is_limited = ("限大额" in sgzt) or ("上限" in sgzt)

        limit_str = sgzt or "未知"
        if sgzt == "限大额" and max_sg is not None and str(max_sg).strip() not in ("", "0", "-1"):
            val = _safe_float(max_sg)
            if val and val >= 10000:
                limit_str = f"限购 {val / 10000:.1f}万"
            elif val:
                limit_str = f"限购 {int(val)}元"

        return {
            "purchase_status": sgzt or "未知",
            "purchase_limit": limit_str,
            "is_paused": is_paused,
            "is_limited": is_limited,
            "nav_fallback": _safe_float(data.get("DWJZ")),
            "nav_date_fallback": data.get("FSRQ") or "",
        }
    except Exception:
        return {"purchase_status": "未知", "purchase_limit": "未知", "is_paused": False, "is_limited": False}


def compute_all_lof_data(force_refresh: bool = False) -> dict[str, Any]:
    now_ts = time.time()
    if not force_refresh and _CACHE["data"] is not None and (now_ts - _CACHE["ts"] < CACHE_TTL):
        cached = dict(_CACHE["data"])
        cached["cached"] = True
        return cached

    codes = [f["code"] for f in FUNDS]

    # 收集所有需要从新浪拉取的外盘/港股/期货/汇率符号
    sina_symbols = {"fx_susdcnh", "hf_NQ", "hf_ES", "hf_CL", "hf_GC", "hf_SI"}
    for f in FUNDS:
        if f.get("benchmark"):
            sina_symbols.add(f["benchmark"])
        if f.get("futures"):
            sina_symbols.add(f["futures"])

    quotes: dict[str, dict[str, Any]] = {}
    em_navs: dict[str, dict[str, Any]] = {}
    sina_data: dict[str, dict[str, Any]] = {}
    tt_navs: dict[str, dict[str, Any]] = {}
    purchase_info: dict[str, dict[str, Any]] = {}

    with ThreadPoolExecutor(max_workers=24) as pool:
        f_quotes = pool.submit(fetch_tencent_quotes, codes)
        f_em_navs = pool.submit(fetch_eastmoney_fund_info, codes)
        f_sina = pool.submit(fetch_sina_global_quotes, sorted(sina_symbols))

        # 仅 A 股 LOF 必须拉天天基金盘中估值；QDII 也可顺带拉作兜底
        f_tt = {pool.submit(fetch_tiantian_nav, c): c for c in codes}
        f_pur = {pool.submit(fetch_eastmoney_purchase_status, c): c for c in codes}

        quotes = f_quotes.result()
        em_navs = f_em_navs.result()
        sina_data = f_sina.result()

        for fut in as_completed(f_tt):
            tt_navs[f_tt[fut]] = fut.result()

        for fut in as_completed(f_pur):
            purchase_info[f_pur[fut]] = fut.result()

    fx_info = sina_data.get("fx_susdcnh") or {}
    fx_chg_pct = fx_info.get("change_pct") or 0.0

    items: list[dict[str, Any]] = []
    for f in FUNDS:
        code = f["code"]
        q = quotes.get(code) or {}
        em = em_navs.get(code) or {}
        tt = tt_navs.get(code) or {}
        pur = purchase_info.get(code) or {}

        price = q.get("price")
        nav = em.get("nav") or tt.get("nav") or pur.get("nav_fallback")
        nav_date = em.get("nav_date") or tt.get("nav_date") or pur.get("nav_date_fallback") or ""

        est_nav: float | None = None
        est_method = ""
        breakdown: dict[str, Any] = {}

        if nav and nav > 0:
            if f["type"] == "a_share":
                gsz = tt.get("gsz")
                if gsz and gsz > 0:
                    est_nav = gsz
                    est_method = "天天基金盘中实时估值"
                    breakdown = {
                        "base_nav": nav,
                        "nav_date": nav_date,
                        "intraday_est_pct": tt.get("gszzl"),
                        "note": "A股标的直接使用天天基金盘中实时估值 (gsz)",
                    }
                else:
                    est_nav = nav
                    est_method = "最新官方净值"
                    breakdown = {"base_nav": nav, "nav_date": nav_date, "note": "暂无盘中估值，使用最新官方净值"}
            else:
                pos = float(f.get("position", 0.92))
                bench_sym = f.get("benchmark")
                fut_sym = f.get("futures")

                bench_info = sina_data.get(bench_sym) if bench_sym else None
                fut_info = sina_data.get(fut_sym) if fut_sym else None

                # 1) 昨夜标的（或港股当日）涨跌幅：仅当标的交易日期晚于官方净值基准日时计入，避免重复计算
                bench_ret = 0.0
                bench_note = "无标的"
                if bench_info and bench_info.get("change_pct") is not None:
                    b_date = bench_info.get("date") or ""
                    b_pct = float(bench_info["change_pct"])
                    if not nav_date or not b_date or b_date > nav_date:
                        bench_ret = b_pct / 100.0
                        bench_note = f"{bench_info.get('name', bench_sym)} {b_pct:+.2f}% ({b_date})"
                    else:
                        bench_ret = 0.0
                        bench_note = f"官方净值已包含 {b_date} 收盘"

                # 2) 盘中股指/商品期货涨跌幅：当期货日期晚于昨夜美股收盘日（且晚于净值日）时叠加
                fut_ret = 0.0
                fut_note = "无期货叠加"
                if fut_info and fut_info.get("change_pct") is not None:
                    f_date = fut_info.get("date") or ""
                    b_date = (bench_info or {}).get("date") or ""
                    f_pct = float(fut_info["change_pct"])
                    if (not b_date or f_date > b_date) and (not nav_date or f_date > nav_date):
                        fut_ret = f_pct / 100.0
                        fut_note = f"{fut_info.get('name', fut_sym)} {f_pct:+.2f}% ({f_date} {fut_info.get('time', '')})"
                    else:
                        fut_ret = 0.0
                        fut_note = f"{fut_sym} 已包含于最新收盘"

                # 3) 离岸人民币汇率日内变化
                fx_ret = fx_chg_pct / 100.0 if fx_chg_pct else 0.0
                fx_note = f"USD/CNH {fx_chg_pct:+.2f}% (现价 {fx_info.get('price', '--')})"

                est_nav = nav * (1.0 + bench_ret * pos) * (1.0 + fut_ret * pos) * (1.0 + fx_ret)
                est_method = "昨夜标的 + 盘中期货 + 离岸汇率"
                breakdown = {
                    "base_nav": nav,
                    "nav_date": nav_date,
                    "position": pos,
                    "benchmark": bench_sym.replace("gb_", "").replace("rt_hk", "").upper() if bench_sym else "--",
                    "benchmark_ret_pct": round(bench_ret * 100.0, 2),
                    "benchmark_note": bench_note,
                    "futures": fut_sym.replace("hf_", "") if fut_sym else "无",
                    "futures_ret_pct": round(fut_ret * 100.0, 2),
                    "futures_note": fut_note,
                    "fx_ret_pct": round(fx_ret * 100.0, 2),
                    "fx_note": fx_note,
                }

        premium_pct: float | None = None
        if price and est_nav and est_nav > 0:
            premium_pct = ((price - est_nav) / est_nav) * 100.0

        is_paused = bool(pur.get("is_paused", False))
        items.append({
            "code": code,
            "name": f["name"],
            "full_name": f["full_name"],
            "category": f["category"],
            "type": f["type"],
            "bench_name": f["bench_name"],
            "price": round(price, 4) if price is not None else None,
            "change_pct": round(q["change_pct"], 2) if q.get("change_pct") is not None else None,
            "turnover_wan": round(q["turnover_wan"], 1) if q.get("turnover_wan") is not None else None,
            "nav": round(nav, 4) if nav is not None else None,
            "nav_date": nav_date,
            "est_nav": round(est_nav, 4) if est_nav is not None else None,
            "premium_pct": round(premium_pct, 2) if premium_pct is not None else None,
            "est_method": est_method,
            "purchase_status": pur.get("purchase_status", "未知"),
            "purchase_limit": pur.get("purchase_limit", "未知"),
            "is_paused": is_paused,
            "breakdown": breakdown,
        })

    items.sort(key=lambda x: (x["premium_pct"] is not None, x["premium_pct"] if x["premium_pct"] is not None else -9999), reverse=True)

    def _macro_item(sym: str, label: str, disp_sym: str) -> dict[str, Any]:
        info = sina_data.get(sym) or {}
        chg = info.get("change_pct")
        return {
            "symbol": disp_sym,
            "label": label,
            "price": round(info["price"], 2) if info.get("price") is not None else None,
            "change_pct": round(chg, 2) if chg is not None else None,
            "date": info.get("date", ""),
        }

    macro = [
        _macro_item("hf_NQ", "纳指100期货", "NQ"),
        _macro_item("hf_ES", "标普500期货", "ES"),
        _macro_item("hf_CL", "WTI原油期货", "CL"),
        _macro_item("hf_GC", "COMEX黄金", "GC"),
        _macro_item("hf_SI", "COMEX白银", "SI"),
        _macro_item("fx_susdcnh", "离岸人民币", "USD/CNH"),
    ]

    valid_premiums = [x["premium_pct"] for x in items if x["premium_pct"] is not None]
    high_premium_cnt = sum(1 for p in valid_premiums if p >= 3.0)
    open_arb_cnt = sum(
        1 for x in items
        if x["premium_pct"] is not None and x["premium_pct"] >= 2.0 and not x["is_paused"]
    )

    payload = {
        "updated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S"),
        "cached": False,
        "count": len(items),
        "summary": {
            "total": len(items),
            "high_premium_count": high_premium_cnt,
            "open_arb_count": open_arb_cnt,
            "max_premium": max(valid_premiums) if valid_premiums else None,
        },
        "macro": macro,
        "items": items,
    }

    _CACHE["ts"] = now_ts
    _CACHE["data"] = payload
    return payload


class handler(BaseHTTPRequestHandler):
    """Vercel Python Serverless Function 入口"""

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        force = qs.get("refresh", ["0"])[0] in ("1", "true", "yes")

        try:
            data = compute_all_lof_data(force_refresh=force)
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "s-maxage=30, stale-while-revalidate=60")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            err = json.dumps({"error": str(e)}, ensure_ascii=False).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(err)
