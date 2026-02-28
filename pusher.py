"""
推送模块 - 统一管理钉钉/飞书/邮件推送
"""

import json
import time
import hmac
import hashlib
import base64
import urllib.parse
import http.client
import logging
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

logger = logging.getLogger("zhiwei-scheduler")


class DingTalkPusher:
    """钉钉 Webhook 推送"""

    def __init__(self, webhook: str, secret: str):
        self.webhook = webhook
        self.secret = secret

    def _sign(self) -> tuple[str, str]:
        """生成签名"""
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            self.secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return timestamp, sign

    def send_markdown(self, title: str, content: str) -> dict:
        """发送 Markdown 消息"""
        try:
            timestamp, sign = self._sign()
            url = f"{self.webhook}&timestamp={timestamp}&sign={sign}"
            parsed = urllib.parse.urlparse(url)

            payload = json.dumps({
                "msgtype": "markdown",
                "markdown": {"title": title, "text": content}
            })

            conn = http.client.HTTPSConnection(parsed.hostname)
            conn.request(
                "POST",
                f"{parsed.path}?{parsed.query}",
                body=payload,
                headers={"Content-Type": "application/json"}
            )
            resp = conn.getresponse()
            result = json.loads(resp.read().decode())
            conn.close()

            if result.get("errcode") == 0:
                logger.info(f"✅ 钉钉推送成功: {title}")
            else:
                logger.error(f"❌ 钉钉推送失败: {result}")
            return result
        except Exception as e:
            logger.error(f"❌ 钉钉推送异常: {e}")
            return {"errcode": -1, "errmsg": str(e)}

    def send_text(self, content: str) -> dict:
        """发送纯文本消息"""
        try:
            timestamp, sign = self._sign()
            url = f"{self.webhook}&timestamp={timestamp}&sign={sign}"
            parsed = urllib.parse.urlparse(url)

            payload = json.dumps({
                "msgtype": "text",
                "text": {"content": content}
            })

            conn = http.client.HTTPSConnection(parsed.hostname)
            conn.request(
                "POST",
                f"{parsed.path}?{parsed.query}",
                body=payload,
                headers={"Content-Type": "application/json"}
            )
            resp = conn.getresponse()
            result = json.loads(resp.read().decode())
            conn.close()
            return result
        except Exception as e:
            logger.error(f"❌ 钉钉文本推送异常: {e}")
            return {"errcode": -1, "errmsg": str(e)}


class FeishuPusher:
    """飞书推送 - 通过飞书 Open API 直接发送"""

    def __init__(self, app_id: str, app_secret: str, chat_id: str, **kwargs):
        self.app_id = app_id
        self.app_secret = app_secret
        self.chat_id = chat_id
        self._token = None
        self._token_expire = 0

    def _get_token(self) -> str:
        """获取 tenant_access_token"""
        if self._token and time.time() < self._token_expire:
            return self._token
        try:
            conn = http.client.HTTPSConnection("open.feishu.cn")
            payload = json.dumps({
                "app_id": self.app_id,
                "app_secret": self.app_secret
            })
            conn.request("POST", "/open-apis/auth/v3/tenant_access_token/internal",
                         body=payload, headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            data = json.loads(resp.read().decode())
            conn.close()
            self._token = data.get("tenant_access_token")
            self._token_expire = time.time() + data.get("expire", 7200) - 300
            return self._token
        except Exception as e:
            logger.error(f"❌ 飞书 token 获取失败: {e}")
            return None

    def send_markdown(self, title: str, content: str) -> dict:
        """直接通过飞书 Open API 发送消息"""
        try:
            token = self._get_token()
            if not token:
                return {"errcode": -1, "errmsg": "token获取失败"}

            text = f"{title}\n\n{content}"
            if len(text) > 4000:
                text = text[:3950] + "\n\n... (内容过长已截断)"

            conn = http.client.HTTPSConnection("open.feishu.cn")
            payload = json.dumps({
                "receive_id": self.chat_id,
                "msg_type": "text",
                "content": json.dumps({"text": text})
            })
            conn.request("POST",
                         "/open-apis/im/v1/messages?receive_id_type=chat_id",
                         body=payload,
                         headers={
                             "Content-Type": "application/json",
                             "Authorization": f"Bearer {token}"
                         })
            resp = conn.getresponse()
            result = json.loads(resp.read().decode())
            conn.close()

            if result.get("code") == 0:
                logger.info(f"✅ 飞书推送成功: {title}")
            else:
                logger.error(f"❌ 飞书推送失败: {result.get('msg')}")
            return result
        except Exception as e:
            logger.error(f"❌ 飞书推送异常: {e}")
            return {"errcode": -1, "errmsg": str(e)}


class EmailPusher:
    """邮件推送（可选）"""

    def __init__(self, smtp_host: str, smtp_port: int, username: str, password: str, receiver: str):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.receiver = receiver

    def send(self, title: str, content: str) -> dict:
        """发送邮件"""
        try:
            msg = MIMEText(content, "html", "utf-8")
            msg["Subject"] = title
            msg["From"] = self.username
            msg["To"] = self.receiver

            with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port) as server:
                server.login(self.username, self.password)
                server.sendmail(self.username, [self.receiver], msg.as_string())

            logger.info(f"✅ 邮件推送成功: {title}")
            return {"errcode": 0}
        except Exception as e:
            logger.error(f"❌ 邮件推送异常: {e}")
            return {"errcode": -1, "errmsg": str(e)}


class PushManager:
    """
    统一推送管理器
    - 管理所有推送渠道
    - 处理静默期
    - 失败重试
    """

    def __init__(self, config: dict):
        self.config = config
        self.pushers = {}
        self._pending_messages = []  # 静默期缓存

        # 初始化钉钉
        dt_conf = config.get("push", {}).get("dingtalk", {})
        if dt_conf.get("enabled"):
            self.pushers["dingtalk"] = DingTalkPusher(
                dt_conf["webhook"], dt_conf["secret"]
            )

        # 初始化飞书
        fs_conf = config.get("push", {}).get("feishu", {})
        if fs_conf.get("enabled"):
            self.pushers["feishu"] = FeishuPusher(
                app_id=fs_conf["app_id"],
                app_secret=fs_conf["app_secret"],
                chat_id=fs_conf["chat_id"]
            )

        # 初始化邮件
        em_conf = config.get("push", {}).get("email", {})
        if em_conf.get("enabled"):
            self.pushers["email"] = EmailPusher(
                em_conf["smtp_host"], em_conf["smtp_port"],
                em_conf["username"], em_conf["password"],
                em_conf["receiver"]
            )

    def is_quiet_hour(self) -> bool:
        """检查当前是否在静默期"""
        qh = self.config.get("quiet_hours", {})
        if not qh.get("enabled"):
            return False

        now = datetime.now()
        current_minutes = now.hour * 60 + now.minute

        start_h, start_m = map(int, qh["start"].split(":"))
        end_h, end_m = map(int, qh["end"].split(":"))
        start_minutes = start_h * 60 + start_m
        end_minutes = end_h * 60 + end_m

        if start_minutes > end_minutes:  # 跨午夜
            return current_minutes >= start_minutes or current_minutes < end_minutes
        else:
            return start_minutes <= current_minutes < end_minutes

    def push(self, title: str, content: str, channels: list[str],
             force: bool = False) -> dict:
        """
        统一推送入口
        - title: 消息标题
        - content: Markdown 内容
        - channels: 推送渠道列表 ["dingtalk", "feishu"]
        - force: 是否忽略静默期
        """
        # 静默期检查
        if not force and self.is_quiet_hour():
            logger.info(f"🌙 静默期，缓存消息: {title}")
            self._pending_messages.append({
                "title": title,
                "content": content,
                "channels": channels,
                "time": datetime.now().isoformat()
            })
            return {"status": "queued", "reason": "quiet_hours"}

        results = {}
        for channel in channels:
            pusher = self.pushers.get(channel)
            if not pusher:
                logger.warning(f"⚠️ 推送渠道未配置: {channel}")
                continue
            result = pusher.send_markdown(title, content)
            results[channel] = result

        return results

    def flush_pending(self):
        """推送静默期内缓存的消息"""
        if not self._pending_messages:
            return

        logger.info(f"📤 推送 {len(self._pending_messages)} 条缓存消息")
        for msg in self._pending_messages:
            self.push(msg["title"], msg["content"], msg["channels"], force=True)
        self._pending_messages.clear()

    def send_alert(self, title: str, content: str):
        """紧急告警 - 忽略静默期，推送所有渠道"""
        self.push(f"🚨 {title}", content, list(self.pushers.keys()), force=True)
