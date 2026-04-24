#!/usr/bin/env python3
"""
Send HTX/Huobi U-margined swap total asset valuation to WeCom.

The reported balance is the `source=valuation` value from
`/linear-swap-api/v1/swap_balance_valuation`, which matches the account-level
total asset valuation used by the balance query script.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from huobi_query_balances import (
    ApiKey,
    DEFAULT_KEYS_FILE,
    DEFAULT_SWAP_HOST,
    get_swap_valuation,
    load_api_keys,
    select_keys,
    signed_post,
    to_decimal,
)


DEFAULT_WEBHOOK_ENV = "WECOM_WEBHOOK_URL"
FALLBACK_WEBHOOK_ENV = "WECHAT_WORK_WEBHOOK_URL"
MAX_WECOM_MARKDOWN_BYTES = 3500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query Huobi U-margined swap valuation balances and send a WeCom webhook report."
    )
    parser.add_argument(
        "--keys",
        default=DEFAULT_KEYS_FILE,
        help=f"Local API key JSON file. Default: {DEFAULT_KEYS_FILE}",
    )
    parser.add_argument(
        "--keys-json-env",
        default="HUOBI_KEYS_JSON",
        help="Environment variable containing the full API key JSON. Takes precedence over --keys.",
    )
    parser.add_argument(
        "--webhook-env",
        default=DEFAULT_WEBHOOK_ENV,
        help=f"Environment variable containing the WeCom webhook URL. Default: {DEFAULT_WEBHOOK_ENV}",
    )
    parser.add_argument(
        "--swap-host",
        default=DEFAULT_SWAP_HOST,
        help=f"U-margined swap API host. Default: {DEFAULT_SWAP_HOST}",
    )
    parser.add_argument(
        "--valuation-asset",
        default="USDT",
        help="Valuation asset to request. Default: USDT",
    )
    parser.add_argument(
        "--position-margin-mode",
        choices=["cross", "isolated", "all"],
        default="all",
        help="U-margined swap position margin mode to query. Default: all",
    )
    parser.add_argument(
        "--position-margin-account",
        default="USDT",
        help="U-margined cross position margin account. Default: USDT",
    )
    parser.add_argument(
        "--max-position-lines",
        type=int,
        default=80,
        help="Maximum position detail lines shown in the WeCom message. Default: 80",
    )
    parser.add_argument(
        "--no-positions",
        action="store_true",
        help="Do not query or report open positions.",
    )
    parser.add_argument(
        "--no",
        action="append",
        help="Only process keys whose 'no' matches this value. Can be passed multiple times.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only process the first N selected keys. Default: no limit.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15,
        help="HTTP timeout in seconds. Default: 15",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Retry count for Huobi and WeCom requests. Default: 2",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Seconds to sleep between Huobi API keys. Default: 0.2",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the report without sending it to WeCom.",
    )
    return parser.parse_args()


def load_keys_from_args(args: argparse.Namespace) -> list[ApiKey]:
    env_json = os.getenv(args.keys_json_env)
    if env_json:
        return parse_api_keys(json.loads(env_json))
    return load_api_keys(args.keys)


def parse_api_keys(raw: Any) -> list[ApiKey]:
    if isinstance(raw, dict) and isinstance(raw.get("items"), list):
        items = raw["items"]
    elif isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        items = [raw]
    else:
        raise ValueError("Unsupported key JSON structure.")

    keys: list[ApiKey] = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        access_key = first_present(item, "access_key", "AccessKeyId", "accessKey", "accessKeyId")
        secret_key = first_present(item, "secret_key", "SecretKey", "secretKey")
        if not access_key or not secret_key:
            continue
        keys.append(
            ApiKey(
                index=index,
                access_key=str(access_key),
                secret_key=str(secret_key),
                no=str(item.get("no", index)),
                uid=str(item.get("uid", "")),
                note=str(item.get("note", "")),
            )
        )
    if not keys:
        raise ValueError("No usable API keys found.")
    return keys


def first_present(item: dict[str, Any], *names: str) -> Any:
    for name in names:
        value = item.get(name)
        if value:
            return value
    return None


def query_report_rows(
    args: argparse.Namespace,
    keys: Iterable[ApiKey],
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[str]]:
    selector_args = SimpleNamespace(no=args.no, limit=args.limit)
    selected_keys = select_keys(selector_args, keys)
    valuation_rows: list[dict[str, str]] = []
    position_rows: list[dict[str, str]] = []
    errors: list[str] = []

    for offset, key in enumerate(selected_keys, start=1):
        prefix = f"no={key.no} uid={key.uid or '-'}"
        print(f"[{offset}/{len(selected_keys)}] querying {prefix} valuation/positions ...", file=sys.stderr)
        try:
            api_valuation_rows = get_swap_valuation(
                key,
                host=args.swap_host,
                valuation_asset=args.valuation_asset.upper(),
                timeout=args.timeout,
                retries=args.retries,
            )
            for row in api_valuation_rows:
                valuation_rows.append(
                    {
                        "no": row.key_no,
                        "uid": row.uid,
                        "asset": row.margin_asset,
                        "balance": format_decimal(row.margin_balance),
                        "note": row.note,
                    }
                )
        except Exception as exc:
            errors.append(f"{prefix} valuation: {exc}")

        if not args.no_positions:
            try:
                position_rows.extend(query_position_rows(args, key))
            except Exception as exc:
                errors.append(f"{prefix} positions: {exc}")

        if args.delay > 0 and offset < len(selected_keys):
            time.sleep(args.delay)

    valuation_rows.sort(key=row_no_sort_key)
    position_rows.sort(key=lambda row: (row_no_sort_key(row), row["contract"], row["direction"]))
    return valuation_rows, position_rows, errors


def query_position_rows(args: argparse.Namespace, key: ApiKey) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if args.position_margin_mode in {"cross", "all"}:
        rows.extend(
            get_swap_position_rows(
                key,
                host=args.swap_host,
                path="/linear-swap-api/v1/swap_cross_position_info",
                body={"margin_account": args.position_margin_account},
                source="cross_position",
                timeout=args.timeout,
                retries=args.retries,
            )
        )
    if args.position_margin_mode in {"isolated", "all"}:
        rows.extend(
            get_swap_position_rows(
                key,
                host=args.swap_host,
                path="/linear-swap-api/v1/swap_position_info",
                body={},
                source="isolated_position",
                timeout=args.timeout,
                retries=args.retries,
            )
        )
    return rows


def get_swap_position_rows(
    key: ApiKey,
    *,
    host: str,
    path: str,
    body: dict[str, Any],
    source: str,
    timeout: float,
    retries: int,
) -> list[dict[str, str]]:
    response = signed_post(
        host=host,
        path=path,
        access_key=key.access_key,
        secret_key=key.secret_key,
        body=body,
        timeout=timeout,
        retries=retries,
    )
    data = response.get("data", [])
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected position response: {response}")

    rows: list[dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        volume = to_decimal(item.get("volume", "0"))
        available = to_decimal(item.get("available", "0"))
        frozen = to_decimal(item.get("frozen", "0"))
        if volume == 0 and available == 0 and frozen == 0:
            continue
        rows.append(
            {
                "no": key.no,
                "uid": key.uid,
                "source": source,
                "margin_mode": str(item.get("margin_mode", "")),
                "contract": str(item.get("contract_code", "")),
                "direction": translate_direction(str(item.get("direction", ""))),
                "volume": format_position_decimal(volume),
                "available": format_position_decimal(available),
                "frozen": format_position_decimal(frozen),
                "cost_open": format_position_decimal(to_decimal(item.get("cost_open", "0"))),
                "cost_hold": format_position_decimal(to_decimal(item.get("cost_hold", "0"))),
                "last_price": format_position_decimal(to_decimal(item.get("last_price", "0"))),
                "profit_unreal": format_decimal(to_decimal(item.get("profit_unreal", "0"))),
                "lever_rate": str(item.get("lever_rate", "")),
            }
        )
    return rows


def row_no_sort_key(row: dict[str, str]) -> int | str:
    return int(row["no"]) if row["no"].isdigit() else row["no"]


def translate_direction(direction: str) -> str:
    mapping = {
        "buy": "多",
        "sell": "空",
    }
    return mapping.get(direction.lower(), direction)


def format_decimal(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.0001")), "f")


def format_position_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    return format(normalized, "f") if value else "0"


def build_markdown(
    rows: list[dict[str, str]],
    position_rows: list[dict[str, str]],
    errors: list[str],
    valuation_asset: str,
    max_position_lines: int,
) -> str:
    now = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S %Z")
    title = f"HTX U本位合约账户总资产 ({valuation_asset.upper()})"

    lines = [f"## {title}", f"> 查询时间：{now}", ""]
    if rows:
        total = sum(Decimal(row["balance"]) for row in rows)
        lines.append(f"账户数：{len(rows)}")
        lines.append(f"合计：<font color=\"info\">{format_decimal(total)} {valuation_asset.upper()}</font>")
        lines.append("")
        # for row in rows:
        #     lines.append(
        #         f"- no {row['no']} / uid {row['uid']}: "
        #         f"<font color=\"comment\">{row['balance']} {row['asset']}</font>"
        #     )
    else:
        lines.append("<font color=\"warning\">没有查询到账户总资产数据。</font>")

    lines.append("")
    lines.append("### 开单/持仓情况")
    if position_rows:
        total_unreal = sum(Decimal(row["profit_unreal"]) for row in position_rows)
        position_accounts = sorted({row["no"] for row in position_rows}, key=lambda no: int(no) if no.isdigit() else no)
        lines.append(f"持仓账号数：{len(position_accounts)}")
        lines.append(f"持仓条数：{len(position_rows)}")
        lines.append(f"未实现盈亏合计：<font color=\"info\">{format_decimal(total_unreal)} {valuation_asset.upper()}</font>")
        lines.append("")
        for row in position_rows[:max_position_lines]:
            lines.append(
                f"- no {row['no']} {row['contract']} {row['direction']} "
                f"{row['volume']}张，均价 {row['cost_hold']}，最新 {row['last_price']}，"
                f"浮盈亏 <font color=\"comment\">{row['profit_unreal']} {valuation_asset.upper()}</font>"
            )
        if len(position_rows) > max_position_lines:
            lines.append(f"- 还有 {len(position_rows) - max_position_lines} 条持仓未展示")
    else:
        lines.append("当前没有查询到开单/持仓。")

    if errors:
        lines.append("")
        lines.append("异常：")
        for error in errors[:10]:
            lines.append(f"- {error}")
        if len(errors) > 10:
            lines.append(f"- 还有 {len(errors) - 10} 条异常未展示")

    return "\n".join(lines)


def send_wecom_markdown(webhook_url: str, markdown: str, *, timeout: float, retries: int) -> None:
    chunks = split_markdown(markdown)
    for index, chunk in enumerate(chunks, start=1):
        content = chunk
        if len(chunks) > 1:
            content = f"**Huobi 报告 {index}/{len(chunks)}**\n\n{chunk}"
        send_wecom_markdown_chunk(webhook_url, content, timeout=timeout, retries=retries)


def split_markdown(markdown: str) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    current_bytes = 0

    for line in markdown.splitlines():
        line_bytes = len((line + "\n").encode("utf-8"))
        if current and current_bytes + line_bytes > MAX_WECOM_MARKDOWN_BYTES:
            chunks.append("\n".join(current))
            current = []
            current_bytes = 0
        current.append(line)
        current_bytes += line_bytes

    if current:
        chunks.append("\n".join(current))
    return chunks or [markdown]


def send_wecom_markdown_chunk(webhook_url: str, markdown: str, *, timeout: float, retries: int) -> None:
    payload = json.dumps(
        {
            "msgtype": "markdown",
            "markdown": {"content": markdown},
        },
        ensure_ascii=False,
    ).encode("utf-8")

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            request = urllib.request.Request(
                webhook_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=timeout) as response:
                data = json.loads(response.read().decode("utf-8"))
            if data.get("errcode") == 0:
                return
            raise RuntimeError(json.dumps(data, ensure_ascii=False))
        except (urllib.error.URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(0.5 * (2**attempt))

    raise RuntimeError(f"WeCom webhook failed: {last_error}")


def webhook_from_env(name: str) -> str:
    webhook_url = os.getenv(name) or os.getenv(FALLBACK_WEBHOOK_ENV)
    if not webhook_url:
        raise ValueError(f"Missing webhook URL env: {name} or {FALLBACK_WEBHOOK_ENV}")
    return webhook_url


def main() -> int:
    args = parse_args()
    try:
        keys = load_keys_from_args(args)
        rows, position_rows, errors = query_report_rows(args, keys)
        markdown = build_markdown(
            rows,
            position_rows,
            errors,
            args.valuation_asset,
            args.max_position_lines,
        )
        print(markdown)

        if not args.dry_run:
            send_wecom_markdown(
                webhook_from_env(args.webhook_env),
                markdown,
                timeout=args.timeout,
                retries=args.retries,
            )
            print("WeCom message sent.", file=sys.stderr)
    except Exception as exc:
        print(f"fatal: {exc}", file=sys.stderr)
        return 1

    return 2 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
