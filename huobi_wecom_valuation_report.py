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
)


DEFAULT_WEBHOOK_ENV = "WECOM_WEBHOOK_URL"
FALLBACK_WEBHOOK_ENV = "WECHAT_WORK_WEBHOOK_URL"


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


def query_valuation_rows(args: argparse.Namespace, keys: Iterable[ApiKey]) -> tuple[list[dict[str, str]], list[str]]:
    selector_args = SimpleNamespace(no=args.no, limit=args.limit)
    selected_keys = select_keys(selector_args, keys)
    rows: list[dict[str, str]] = []
    errors: list[str] = []

    for offset, key in enumerate(selected_keys, start=1):
        prefix = f"no={key.no} uid={key.uid or '-'}"
        print(f"[{offset}/{len(selected_keys)}] querying {prefix} valuation ...", file=sys.stderr)
        try:
            valuation_rows = get_swap_valuation(
                key,
                host=args.swap_host,
                valuation_asset=args.valuation_asset.upper(),
                timeout=args.timeout,
                retries=args.retries,
            )
            for row in valuation_rows:
                rows.append(
                    {
                        "no": row.key_no,
                        "uid": row.uid,
                        "asset": row.margin_asset,
                        "balance": format_decimal(row.margin_balance),
                        "note": row.note,
                    }
                )
        except Exception as exc:
            errors.append(f"{prefix}: {exc}")

        if args.delay > 0 and offset < len(selected_keys):
            time.sleep(args.delay)

    rows.sort(key=lambda row: int(row["no"]) if row["no"].isdigit() else row["no"])
    return rows, errors


def format_decimal(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.0001")), "f")


def build_markdown(rows: list[dict[str, str]], errors: list[str], valuation_asset: str) -> str:
    now = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S %Z")
    title = f"HTX U本位合约账户总资产 ({valuation_asset.upper()})"

    lines = [f"## {title}", f"> 查询时间：{now}", ""]
    if rows:
        total = sum(Decimal(row["balance"]) for row in rows)
        lines.append(f"账户数：{len(rows)}")
        lines.append(f"合计：<font color=\"info\">{format_decimal(total)} {valuation_asset.upper()}</font>")
        lines.append("")
        for row in rows:
            lines.append(
                f"- no {row['no']} / uid {row['uid']}: "
                f"<font color=\"comment\">{row['balance']} {row['asset']}</font>"
            )
    else:
        lines.append("<font color=\"warning\">没有查询到账户总资产数据。</font>")

    if errors:
        lines.append("")
        lines.append("异常：")
        for error in errors[:10]:
            lines.append(f"- {error}")
        if len(errors) > 10:
            lines.append(f"- 还有 {len(errors) - 10} 条异常未展示")

    return "\n".join(lines)


def send_wecom_markdown(webhook_url: str, markdown: str, *, timeout: float, retries: int) -> None:
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
        rows, errors = query_valuation_rows(args, keys)
        markdown = build_markdown(rows, errors, args.valuation_asset)
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
