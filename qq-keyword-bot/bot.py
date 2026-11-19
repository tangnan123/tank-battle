#!/usr/bin/env python3
"""QQ 关键词自动回复机器人（OneBot v11 协议）。

监听 QQ 消息，当消息文本包含关键词（默认「量化」）时，
用当前登录的 QQ 账号回复指定内容（默认「1」）。

依赖一个 OneBot v11 实现作为 QQ 登录端与协议端，例如 NapCat、Lagrange，
本脚本以「正向 WebSocket 客户端」的方式连接它。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from collections import deque

import websockets

# ---------------- 配置（均可用环境变量覆盖） ----------------
WS_URL = os.getenv("ONEBOT_WS_URL", "ws://127.0.0.1:3001")
ACCESS_TOKEN = os.getenv("ONEBOT_ACCESS_TOKEN", "")
KEYWORD = os.getenv("BOT_KEYWORD", "量化")
REPLY_TEXT = os.getenv("BOT_REPLY_TEXT", "1")
REPLY_PRIVATE = os.getenv("REPLY_PRIVATE", "true").lower() != "false"
REPLY_GROUP = os.getenv("REPLY_GROUP", "true").lower() != "false"
AT_SENDER = os.getenv("AT_SENDER", "false").lower() == "true"
COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", "0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("qq-bot")

CQ_PATTERN = re.compile(r"\[CQ:[^\]]*\]")
_seen_message_ids: deque[str] = deque(maxlen=500)
_last_reply_at: dict[str, float] = {}


def extract_text(message) -> str:
    """从 OneBot 消息（CQ 码字符串或消息段数组）中提取纯文本。"""
    if isinstance(message, str):
        return CQ_PATTERN.sub("", message)
    parts = []
    for seg in message or []:
        if isinstance(seg, dict) and seg.get("type") == "text":
            parts.append(seg.get("data", {}).get("text", ""))
    return "".join(parts)


def build_reply(event: dict) -> list[dict]:
    """构造回复消息段。"""
    segments: list[dict] = []
    if event.get("message_type") == "group" and AT_SENDER:
        segments.append(
            {"type": "at", "data": {"qq": str(event.get("user_id"))}}
        )
        segments.append({"type": "text", "data": {"text": " "}})
    segments.append({"type": "text", "data": {"text": REPLY_TEXT}})
    return segments


async def send_reply(ws, event: dict) -> None:
    message_type = event.get("message_type")
    params: dict = {"message": build_reply(event)}
    if message_type == "group":
        params["message_type"] = "group"
        params["group_id"] = event.get("group_id")
    else:
        params["message_type"] = "private"
        params["user_id"] = event.get("user_id")

    payload = {"action": "send_msg", "params": params, "echo": "reply"}
    await ws.send(json.dumps(payload, ensure_ascii=False))
    log.info(
        "已回复 %s：%s",
        f"群 {event.get('group_id')}" if message_type == "group"
        else f"用户 {event.get('user_id')}",
        REPLY_TEXT,
    )


async def handle_event(ws, event: dict) -> None:
    if event.get("post_type") != "message":
        return

    message_type = event.get("message_type")
    if message_type == "group" and not REPLY_GROUP:
        return
    if message_type == "private" and not REPLY_PRIVATE:
        return

    # 忽略自己发出的消息，避免自触发
    if event.get("user_id") == event.get("self_id"):
        return

    message_id = str(event.get("message_id", ""))
    if message_id:
        if message_id in _seen_message_ids:
            return
        _seen_message_ids.append(message_id)

    text = extract_text(event.get("message"))
    if KEYWORD not in text:
        return

    if COOLDOWN_SECONDS > 0:
        target = f"{message_type}:{event.get('group_id') or event.get('user_id')}"
        now = time.monotonic()
        if now - _last_reply_at.get(target, 0) < COOLDOWN_SECONDS:
            log.info("冷却中，跳过 %s", target)
            return
        _last_reply_at[target] = now

    log.info("命中关键词「%s」：%s", KEYWORD, text[:80])
    await send_reply(ws, event)


async def run() -> None:
    headers = {}
    if ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {ACCESS_TOKEN}"

    delay = 1.0
    while True:
        try:
            log.info("正在连接 %s ...", WS_URL)
            async with websockets.connect(
                WS_URL, additional_headers=headers, ping_interval=30
            ) as ws:
                log.info("已连接，等待消息")
                delay = 1.0
                async for raw in ws:
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(event, dict):
                        continue
                    try:
                        await handle_event(ws, event)
                    except Exception:  # 单条消息异常不影响主循环
                        log.exception("处理消息失败")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("连接断开：%s，%.0f 秒后重连", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("已停止")


if __name__ == "__main__":
    main()
