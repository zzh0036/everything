"""
飞书话题群 → 按日汇总为 TXT（用户问题 / 讨论摘要 / 时间等）

说明（工程上必须先确认的两点）：
1. 在飞书开放平台创建企业自建应用，开通 IM 相关权限（如读取群消息历史，以控制台实际名称为准），
   将应用机器人拉进目标「话题群」，并拿到该群的 chat_id。
2. 话题群内一条「话题」对应 thread_id；通常需先按群拉消息拿到根帖与 thread_id，
   再按 thread 拉该话题下回复。具体参数以官方文档为准：
   https://open.feishu.cn/document/server-docs/im-v1/message/list

运行：配置 .env 后
  pip install -r requirements.txt
  python daily_summary.py

定时：Windows 任务计划程序 / Linux cron / CI 定时任务，每天固定时间执行本脚本。
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# 中国时区用于「按自然日」切分（可按需改为 UTC）
TZ_CN = timezone(timedelta(hours=8))


def load_env() -> dict:
    """极简读 .env，避免额外依赖；也可用 python-dotenv。"""
    path = Path(__file__).resolve().parent / ".env"
    out = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def get_tenant_access_token(app_id: str, app_secret: str) -> str:
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    r = requests.post(url, json={"app_id": app_id, "app_secret": app_secret}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取 tenant_access_token 失败: {data}")
    return data["tenant_access_token"]


def feishu_get(token: str, path: str, params: dict | None = None) -> dict:
    url = "https://open.feishu.cn/open-apis" + path
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, params=params or {}, timeout=60)
    r.raise_for_status()
    body = r.json()
    if body.get("code") != 0:
        raise RuntimeError(f"请求失败 {path}: {body}")
    return body


def ms_to_cn_str(ms: str | int | None) -> str:
    if ms is None:
        return ""
    try:
        ts = int(ms) / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(TZ_CN)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        return str(ms)


def list_messages_in_chat(token: str, chat_id: str, page_size: int = 50) -> list[dict]:
    """拉取群内消息（分页）。话题根帖一般会带 thread_id，供后续按话题拉线程内消息。"""
    items: list[dict] = []
    page_token: str | None = None
    while True:
        params: dict = {
            "container_id_type": "chat",
            "container_id": chat_id,
            "sort_type": "ByCreateTimeDesc",
            "page_size": page_size,
        }
        if page_token:
            params["page_token"] = page_token
        body = feishu_get(token, "/im/v1/messages", params)
        data = body.get("data") or {}
        batch = data.get("items") or []
        items.extend(batch)
        page_token = data.get("page_token")
        if not page_token:
            break
    return items


def list_messages_in_thread(token: str, thread_id: str, page_size: int = 50) -> list[dict]:
    items: list[dict] = []
    page_token: str | None = None
    while True:
        params: dict = {
            "container_id_type": "thread",
            "container_id": thread_id,
            "sort_type": "ByCreateTimeAsc",
            "page_size": page_size,
        }
        if page_token:
            params["page_token"] = page_token
        body = feishu_get(token, "/im/v1/messages", params)
        data = body.get("data") or {}
        batch = data.get("items") or []
        items.extend(batch)
        page_token = data.get("page_token")
        if not page_token:
            break
    return items


def message_text_preview(item: dict) -> str:
    """从消息体里取一段可读文本（不同 msg_type 需分别解析，此处仅处理常见 text/post 的简化逻辑）。"""
    body = item.get("body") or {}
    content = body.get("content")
    if not content:
        return ""
    try:
        obj = json.loads(content) if isinstance(content, str) else content
    except json.JSONDecodeError:
        return str(content)[:500]
    if isinstance(obj, dict) and "text" in obj:
        return str(obj.get("text", ""))[:2000]
    return str(obj)[:2000]


def filter_today_threads(
    chat_messages: list[dict],
    day: datetime,
) -> dict[str, dict]:
    """
    从群内消息中找出「当天有活动」的话题 thread_id。
    策略：根帖或任意消息的创建时间落在当天即纳入（可按产品改为「仅根帖日期」）。
    """
    day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    t0 = int(day_start.timestamp() * 1000)
    t1 = int(day_end.timestamp() * 1000)

    threads: dict[str, dict] = {}
    for m in chat_messages:
        tid = m.get("thread_id") or m.get("message_id")
        if not tid:
            continue
        created = m.get("create_time")
        try:
            ct = int(created) if created is not None else 0
        except (TypeError, ValueError):
            ct = 0
        if not (t0 <= ct < t1):
            continue
        if tid not in threads:
            threads[tid] = {"root_hint": m}
    return threads


def build_topic_record(token: str, thread_id: str) -> dict:
    msgs = list_messages_in_thread(token, thread_id)
    lines: list[str] = []
    for m in msgs:
        who = (m.get("sender", {}) or {}).get("sender_id", {}).get("open_id", "?")
        when = ms_to_cn_str(m.get("create_time"))
        text = message_text_preview(m)
        lines.append(f"[{when}] {who}: {text}")

    first = msgs[0] if msgs else {}
    return {
        "thread_id": thread_id,
        "first_time_cn": ms_to_cn_str(first.get("create_time")),
        "raw_dialogue": "\n".join(lines),
        "message_count": len(msgs),
    }


def summarize_with_rules(record: dict) -> dict:
    """
    无大模型时的占位：把首条当「问题」、末条或人工规则当「结论」。
    接入 GPT/Claude 时：把 raw_dialogue 拼进 prompt，让模型输出 JSON 再写入 TXT。
    """
    dialogue = record.get("raw_dialogue") or ""
    parts = dialogue.split("\n")
    question = parts[0] if parts else ""
    resolution = parts[-1] if len(parts) > 1 else "（单条或无回复，待补充）"
    return {
        "用户问题/话题标题（启发式）": question,
        "解决方式/最后一条（启发式）": resolution,
        "话题首条时间": record.get("first_time_cn", ""),
        "thread_id": record.get("thread_id", ""),
        "消息条数": str(record.get("message_count", 0)),
    }


def write_txt(output_dir: Path, day: datetime, rows: list[dict]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    name = day.strftime("%Y-%m-%d")
    path = output_dir / f"话题日结_{name}.txt"
    lines: list[str] = [
        f"日期: {name}（Asia/Shanghai）",
        f"生成时间: {datetime.now(tz=TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "=" * 60,
        "",
    ]
    for i, row in enumerate(rows, 1):
        lines.append(f"【话题 {i}】")
        for k, v in row.items():
            lines.append(f"{k}: {v}")
        lines.append("")
        lines.append("-" * 40)
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def main() -> None:
    env = {**os.environ, **load_env()}
    app_id = env.get("FEISHU_APP_ID", "").strip()
    app_secret = env.get("FEISHU_APP_SECRET", "").strip()
    chat_id = env.get("FEISHU_TOPIC_GROUP_CHAT_ID", "").strip()
    if not (app_id and app_secret and chat_id):
        raise SystemExit(
            "请复制 .env.example 为 .env 并填写 FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_TOPIC_GROUP_CHAT_ID"
        )

    token = get_tenant_access_token(app_id, app_secret)
    chat_messages = list_messages_in_chat(token, chat_id)

    # 默认：总结「今天」；也可改为 argparse 传日期
    today = datetime.now(tz=TZ_CN)
    thread_map = filter_today_threads(chat_messages, today)

    # 若群内当天没有新根帖，话题可能只在 thread 内更新：可改为遍历近期 thread_id 列表（需结合你们群形态调整）
    rows: list[dict] = []
    for tid in sorted(thread_map.keys()):
        rec = build_topic_record(token, tid)
        rows.append(summarize_with_rules(rec))

    out_dir = Path(__file__).resolve().parent / "output"
    path = write_txt(out_dir, today, rows)
    print(f"已写入: {path}")


if __name__ == "__main__":
    main()
