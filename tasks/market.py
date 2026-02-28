"""
美股/加密货币数据采集模块
"""

import json
import http.client
import ssl
import logging
from datetime import datetime, timezone

logger = logging.getLogger("zhiwei-scheduler")


# ============ 美股 ============

def fetch_us_market(symbols: list) -> dict:
    """
    获取美股行情
    使用 Yahoo Finance 非官方 API
    """
    results = {"time": datetime.now().strftime("%Y-%m-%d %H:%M"), "stocks": []}

    try:
        symbol_str = ",".join(symbols)
        ctx = ssl.create_default_context()
        conn = http.client.HTTPSConnection("query1.finance.yahoo.com", context=ctx)
        conn.request("GET", f"/v7/finance/quote?symbols={symbol_str}",
                     headers={"User-Agent": "Mozilla/5.0"})
        resp = conn.getresponse()

        if resp.status == 200:
            data = json.loads(resp.read().decode())
            quotes = data.get("quoteResponse", {}).get("result", [])

            for q in quotes:
                stock = {
                    "symbol": q.get("symbol", ""),
                    "name": q.get("shortName", q.get("longName", "")),
                    "price": q.get("regularMarketPrice", 0),
                    "change": q.get("regularMarketChange", 0),
                    "change_pct": q.get("regularMarketChangePercent", 0),
                    "volume": q.get("regularMarketVolume", 0),
                    "market_cap": q.get("marketCap", 0),
                    "state": q.get("marketState", ""),
                }
                results["stocks"].append(stock)

        conn.close()
        logger.info(f"✅ 美股数据获取成功: {len(results['stocks'])} 只")

    except Exception as e:
        logger.error(f"❌ 美股数据获取失败: {e}")
        results["error"] = str(e)

    return results


def format_market_markdown(data: dict, report_type: str = "open") -> str:
    """格式化美股行情为 Markdown"""
    if "error" in data:
        return f"⚠️ 美股数据获取失败: {data['error']}"

    title = "🔔 美股开盘播报" if report_type == "open" else "📊 美股收盘复盘"
    lines = [f"### {title}", f"*{data['time']}*\n"]

    # 指数在前
    indices = [s for s in data["stocks"] if s["symbol"].startswith("^")]
    stocks = [s for s in data["stocks"] if not s["symbol"].startswith("^")]

    if indices:
        lines.append("**主要指数**\n")
        for s in indices:
            emoji = "🔴" if s["change"] < 0 else "🟢"
            pct = f"{s['change_pct']:+.2f}%"
            lines.append(f"- {emoji} **{s['name']}**: {s['price']:.2f} ({pct})")
        lines.append("")

    if stocks:
        lines.append("**重点个股**\n")
        # 按涨跌幅排序
        stocks.sort(key=lambda x: x["change_pct"], reverse=True)
        for s in stocks:
            emoji = "🔴" if s["change"] < 0 else "🟢"
            pct = f"{s['change_pct']:+.2f}%"
            lines.append(f"- {emoji} {s['symbol']} ({s['name']}): ${s['price']:.2f} ({pct})")

    return "\n".join(lines)


# ============ 加密货币 ============

def fetch_crypto(symbols: list) -> dict:
    """
    获取加密货币行情
    使用 CoinGecko 免费 API
    """
    results = {"time": datetime.now().strftime("%Y-%m-%d %H:%M"), "coins": [], "alerts": []}

    # CoinGecko ID 映射
    id_map = {
        "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
        "BNB": "binancecoin", "XRP": "ripple", "DOGE": "dogecoin",
        "ADA": "cardano", "AVAX": "avalanche-2", "DOT": "polkadot",
        "MATIC": "matic-network", "LINK": "chainlink"
    }

    ids = [id_map.get(s, s.lower()) for s in symbols]

    try:
        ctx = ssl.create_default_context()
        conn = http.client.HTTPSConnection("api.coingecko.com", context=ctx)
        conn.request(
            "GET",
            f"/api/v3/simple/price?ids={','.join(ids)}&vs_currencies=usd&include_24hr_change=true&include_market_cap=true",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp = conn.getresponse()

        if resp.status == 200:
            data = json.loads(resp.read().decode())

            for symbol, cg_id in zip(symbols, ids):
                if cg_id in data:
                    coin_data = data[cg_id]
                    price = coin_data.get("usd", 0)
                    change_24h = coin_data.get("usd_24h_change", 0)

                    coin = {
                        "symbol": symbol,
                        "price": price,
                        "change_24h": change_24h,
                        "market_cap": coin_data.get("usd_market_cap", 0)
                    }
                    results["coins"].append(coin)

                    # 预警检查
                    if abs(change_24h) >= 5:
                        results["alerts"].append({
                            "symbol": symbol,
                            "price": price,
                            "change": change_24h,
                            "level": "critical" if abs(change_24h) >= 10 else "warning"
                        })

        conn.close()
        logger.info(f"✅ 加密货币数据获取成功: {len(results['coins'])} 个")

    except Exception as e:
        logger.error(f"❌ 加密货币数据获取失败: {e}")
        results["error"] = str(e)

    return results


def format_crypto_markdown(data: dict, alert_threshold: float = 5.0) -> str:
    """格式化加密货币行情为 Markdown"""
    if "error" in data:
        return f"⚠️ 加密货币数据获取失败: {data['error']}"

    lines = [f"### 🪙 加密货币行情", f"*{data['time']}*\n"]

    for coin in data["coins"]:
        emoji = "🔴" if coin["change_24h"] < 0 else "🟢"
        alert = " ⚠️" if abs(coin["change_24h"]) >= alert_threshold else ""
        pct = f"{coin['change_24h']:+.2f}%"
        lines.append(f"- {emoji} **{coin['symbol']}**: ${coin['price']:,.2f} ({pct}){alert}")

    # 预警信息
    if data.get("alerts"):
        lines.append("\n**⚠️ 预警**\n")
        for a in data["alerts"]:
            level = "🚨" if a["level"] == "critical" else "⚠️"
            lines.append(f"- {level} {a['symbol']} 24h变动 {a['change']:+.2f}%")

    return "\n".join(lines)
