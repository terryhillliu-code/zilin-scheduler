"""
天气数据采集 - 杭州
使用 wttr.in 免费 API
"""

import json
import http.client
import logging
from datetime import datetime

logger = logging.getLogger("zhiwei-scheduler")

# Fallback 消息
FALLBACK_MESSAGE = "今日天气数据暂不可用，请参考窗外实际情况"


def fetch_weather(city: str = "杭州", timeout: int = 5) -> dict:
    """
    获取天气数据
    返回: {"city": "杭州", "temp": "25°C", "condition": "晴", "humidity": "60%", ...}

    Args:
        city: 城市名称
        timeout: 请求超时时间（秒）
    """
    try:
        conn = http.client.HTTPSConnection("wttr.in", timeout=timeout)
        # 使用中文格式
        conn.request("GET", f"/{city}?format=j1", headers={"Accept-Language": "zh"})
        resp = conn.getresponse()
        data = json.loads(resp.read().decode())
        conn.close()

        current = data.get("current_condition", [{}])[0]
        forecast = data.get("weather", [])

        result = {
            "city": city,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "temp": f"{current.get('temp_C', 'N/A')}°C",
            "feels_like": f"{current.get('FeelsLikeC', 'N/A')}°C",
            "condition": current.get("lang_zh", [{}])[0].get("value", current.get("weatherDesc", [{}])[0].get("value", "未知")),
            "humidity": f"{current.get('humidity', 'N/A')}%",
            "wind": f"{current.get('winddir16Point', '')} {current.get('windspeedKmph', '')}km/h",
            "visibility": f"{current.get('visibility', 'N/A')}km",
            "uv_index": current.get("uvIndex", "N/A"),
            "forecast": []
        }

        # 未来 3 天预报
        for day in forecast[:3]:
            result["forecast"].append({
                "date": day.get("date", ""),
                "max_temp": f"{day.get('maxtempC', 'N/A')}°C",
                "min_temp": f"{day.get('mintempC', 'N/A')}°C",
                "condition": day.get("hourly", [{}])[4].get("lang_zh", [{}])[0].get("value", "未知") if day.get("hourly") else "未知"
            })

        logger.info(f"✅ 天气获取成功: {city} {result['temp']} {result['condition']}")
        return result

    except http.client.HTTPException as e:
        logger.error(f"❌ 天气获取失败 (HTTP): {e}")
        return {"city": city, "error": str(e), "fallback": FALLBACK_MESSAGE}
    except TimeoutError as e:
        logger.error(f"❌ 天气获取超时: {e}")
        return {"city": city, "error": "timeout", "fallback": FALLBACK_MESSAGE}
    except json.JSONDecodeError as e:
        logger.error(f"❌ 天气数据解析失败: {e}")
        return {"city": city, "error": "parse_error", "fallback": FALLBACK_MESSAGE}
    except Exception as e:
        logger.error(f"❌ 天气获取失败: {e}")
        return {"city": city, "error": str(e), "fallback": FALLBACK_MESSAGE}


def format_weather_markdown(data: dict, brief: bool = False) -> str:
    """格式化天气为 Markdown"""
    if "fallback" in data:
        return f"⚠️ {data['fallback']}"

    if "error" in data:
        return f"⚠️ 天气获取失败: {data['error']}"

    if brief:
        return f"🌤 **{data['city']}**: {data['temp']}（体感{data['feels_like']}），{data['condition']}，湿度{data['humidity']}"

    lines = [
        f"### 🌤 {data['city']}天气",
        f"- 🌡 温度: {data['temp']}（体感 {data['feels_like']}）",
        f"- 🌈 天气: {data['condition']}",
        f"- 💧 湿度: {data['humidity']}",
        f"- 🌬 风力: {data['wind']}",
        f"- ☀️ UV指数: {data['uv_index']}",
    ]

    if data.get("forecast"):
        lines.append("\n**未来预报**")
        for f in data["forecast"]:
            lines.append(f"- {f['date']}: {f['min_temp']}~{f['max_temp']} {f['condition']}")

    return "\n".join(lines)
