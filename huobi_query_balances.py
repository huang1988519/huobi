#!/usr/bin/env python3
"""
Query HTX/Huobi account balances from a JSON file containing API keys.

The script expects the key file to contain either:
  - a top-level "items" list, where each item has access_key and secret_key
  - a plain list of key objects
  - a single key object

Example:
  python3 huobi_query_balances.py \
    --keys huobi_readonly_subkeys_cawws_501_550_20260423_001613.json

  python3 huobi_query_balances.py --market usdt-swap --no 520
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import hmac
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


DEFAULT_KEYS_FILE = "huobi_readonly_subkeys_cawws_501_550_20260423_001613.json"
DEFAULT_HOST = "api.huobi.pro"
DEFAULT_SWAP_HOST = "api.hbdm.com"


class HuobiApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class ApiKey:
    index: int
    access_key: str
    secret_key: str
    no: str
    uid: str
    note: str


@dataclass(frozen=True)
class BalanceRow:
    key_no: str
    uid: str
    note: str
    account_id: str
    account_type: str
    account_state: str
    currency: str
    trade: Decimal
    frozen: Decimal

    @property
    def total(self) -> Decimal:
        return self.trade + self.frozen


@dataclass(frozen=True)
class SwapBalanceRow:
    key_no: str
    uid: str
    note: str
    source: str
    margin_mode: str
    margin_account: str
    margin_asset: str
    contract_code: str
    margin_balance: Decimal
    margin_static: Decimal
    margin_available: Decimal
    margin_frozen: Decimal
    margin_position: Decimal
    profit_unreal: Decimal
    withdraw_available: Decimal


@dataclass(frozen=True)
class QueryResult:
    spot_rows: list[BalanceRow]
    swap_rows: list[SwapBalanceRow]
    errors: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query HTX/Huobi account balances with signed REST API requests."
    )
    parser.add_argument(
        "--keys",
        default=DEFAULT_KEYS_FILE,
        help=f"API key JSON file. Default: {DEFAULT_KEYS_FILE}",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"API host used for signing and requests. Default: {DEFAULT_HOST}",
    )
    parser.add_argument(
        "--swap-host",
        default=DEFAULT_SWAP_HOST,
        help=f"U-margined swap API host used for signing and requests. Default: {DEFAULT_SWAP_HOST}",
    )
    parser.add_argument(
        "--market",
        choices=["spot", "usdt-swap", "all"],
        default="spot",
        help="Which market/account API to query. Default: spot",
    )
    parser.add_argument(
        "--account-type",
        default="spot",
        help="Only query accounts of this type. Use 'all' to query every account type. Default: spot",
    )
    parser.add_argument(
        "--swap-margin-mode",
        choices=["cross", "isolated", "all"],
        default="all",
        help="U-margined swap margin mode to query. Default: all",
    )
    parser.add_argument(
        "--swap-margin-account",
        default="USDT",
        help="U-margined cross margin account. Default: USDT",
    )
    parser.add_argument(
        "--contract-code",
        help="U-margined isolated contract code, e.g. BTC-USDT. If omitted, all isolated accounts are returned.",
    )
    parser.add_argument(
        "--currency",
        action="append",
        help="Only show a currency, e.g. --currency usdt. Can be passed multiple times.",
    )
    parser.add_argument(
        "--include-zero",
        action="store_true",
        help="Include zero balances. By default only non-zero balances are printed.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only process the first N keys, useful for testing. Default: no limit.",
    )
    parser.add_argument(
        "--no",
        action="append",
        help="Only process the key whose 'no' matches this value. Can be passed multiple times.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Seconds to sleep between keys to reduce rate-limit risk. Default: 0.2",
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
        help="Retry count for transient HTTP/API failures. Default: 2",
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        help="Optional CSV output path.",
    )
    parser.add_argument(
        "--json",
        dest="json_path",
        help="Optional JSON output path.",
    )
    return parser.parse_args()


def load_api_keys(path: str) -> list[ApiKey]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
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
            print(f"[skip] item #{index}: missing access_key/secret_key", file=sys.stderr)
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


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def encode_params(params: dict[str, Any]) -> str:
    ordered = sorted((str(k), str(v)) for k, v in params.items())
    return urllib.parse.urlencode(ordered, quote_via=urllib.parse.quote)


def sign_request(
    *,
    method: str,
    host: str,
    path: str,
    params: dict[str, Any],
    secret_key: str,
) -> dict[str, str]:
    signing_params = {
        "AccessKeyId": params.pop("AccessKeyId"),
        "SignatureMethod": "HmacSHA256",
        "SignatureVersion": "2",
        "Timestamp": utc_timestamp(),
        **params,
    }
    encoded = encode_params(signing_params)
    payload = "\n".join([method.upper(), host.lower(), path, encoded])
    digest = hmac.new(secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    signing_params["Signature"] = base64.b64encode(digest).decode("utf-8")
    return {k: str(v) for k, v in signing_params.items()}


def signed_get(
    *,
    host: str,
    path: str,
    access_key: str,
    secret_key: str,
    params: dict[str, Any] | None = None,
    timeout: float,
    retries: int,
) -> dict[str, Any]:
    return signed_request(
        method="GET",
        host=host,
        path=path,
        access_key=access_key,
        secret_key=secret_key,
        params=params,
        timeout=timeout,
        retries=retries,
    )


def signed_post(
    *,
    host: str,
    path: str,
    access_key: str,
    secret_key: str,
    body: dict[str, Any] | None = None,
    timeout: float,
    retries: int,
) -> dict[str, Any]:
    return signed_request(
        method="POST",
        host=host,
        path=path,
        access_key=access_key,
        secret_key=secret_key,
        body=body,
        timeout=timeout,
        retries=retries,
    )


def signed_request(
    *,
    method: str,
    host: str,
    path: str,
    access_key: str,
    secret_key: str,
    params: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
    timeout: float,
    retries: int,
) -> dict[str, Any]:
    params = dict(params or {})
    params["AccessKeyId"] = access_key

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            signed_params = sign_request(
                method=method,
                host=host,
                path=path,
                params=params.copy(),
                secret_key=secret_key,
            )
            query = encode_params(signed_params)
            url = f"https://{host}{path}?{query}"
            request_body = None
            headers = {
                "Accept": "application/json",
                "User-Agent": "huobi-balance-query/1.0",
            }
            if method.upper() == "POST":
                request_body = json.dumps(body or {}, separators=(",", ":")).encode("utf-8")
                headers["Content-Type"] = "application/json"

            request = urllib.request.Request(
                url,
                data=request_body,
                headers=headers,
                method=method.upper(),
            )
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read().decode("utf-8")

            data = json.loads(body, parse_float=str)
            if data.get("status") == "ok" or data.get("code") == 200:
                return data
            raise HuobiApiError(json.dumps(data, ensure_ascii=False))
        except (urllib.error.URLError, TimeoutError, HuobiApiError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(0.5 * (2**attempt))

    raise HuobiApiError(str(last_error))


def get_accounts(key: ApiKey, *, host: str, timeout: float, retries: int) -> list[dict[str, Any]]:
    response = signed_get(
        host=host,
        path="/v1/account/accounts",
        access_key=key.access_key,
        secret_key=key.secret_key,
        timeout=timeout,
        retries=retries,
    )
    accounts = response.get("data", [])
    if not isinstance(accounts, list):
        raise HuobiApiError(f"Unexpected accounts response: {response}")
    return [account for account in accounts if isinstance(account, dict)]


def get_balance(
    key: ApiKey,
    account: dict[str, Any],
    *,
    host: str,
    timeout: float,
    retries: int,
) -> list[BalanceRow]:
    account_id = str(account["id"])
    response = signed_get(
        host=host,
        path=f"/v1/account/accounts/{account_id}/balance",
        access_key=key.access_key,
        secret_key=key.secret_key,
        timeout=timeout,
        retries=retries,
    )
    data = response.get("data", {})
    items = data.get("list", []) if isinstance(data, dict) else []
    grouped: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))

    for item in items:
        if not isinstance(item, dict):
            continue
        currency = str(item.get("currency", "")).lower()
        balance_type = str(item.get("type", "")).lower()
        if not currency or not balance_type:
            continue
        grouped[currency][balance_type] += to_decimal(item.get("balance", "0"))

    rows: list[BalanceRow] = []
    for currency in sorted(grouped):
        rows.append(
            BalanceRow(
                key_no=key.no,
                uid=key.uid,
                note=key.note,
                account_id=account_id,
                account_type=str(account.get("type", "")),
                account_state=str(account.get("state", "")),
                currency=currency,
                trade=grouped[currency].get("trade", Decimal("0")),
                frozen=grouped[currency].get("frozen", Decimal("0")),
            )
        )
    return rows


def get_swap_cross_balance(
    key: ApiKey,
    *,
    host: str,
    margin_account: str,
    timeout: float,
    retries: int,
) -> list[SwapBalanceRow]:
    response = signed_post(
        host=host,
        path="/linear-swap-api/v1/swap_cross_account_info",
        access_key=key.access_key,
        secret_key=key.secret_key,
        body={"margin_account": margin_account} if margin_account else {},
        timeout=timeout,
        retries=retries,
    )
    data = response.get("data", [])
    if not isinstance(data, list):
        raise HuobiApiError(f"Unexpected swap cross response: {response}")
    return [swap_row_from_item(key, "cross_account", item) for item in data if isinstance(item, dict)]


def get_swap_isolated_balance(
    key: ApiKey,
    *,
    host: str,
    contract_code: str | None,
    timeout: float,
    retries: int,
) -> list[SwapBalanceRow]:
    body = {"contract_code": contract_code} if contract_code else {}
    response = signed_post(
        host=host,
        path="/linear-swap-api/v1/swap_account_info",
        access_key=key.access_key,
        secret_key=key.secret_key,
        body=body,
        timeout=timeout,
        retries=retries,
    )
    data = response.get("data", [])
    if not isinstance(data, list):
        raise HuobiApiError(f"Unexpected swap isolated response: {response}")
    return [swap_row_from_item(key, "isolated_account", item) for item in data if isinstance(item, dict)]


def get_swap_valuation(
    key: ApiKey,
    *,
    host: str,
    valuation_asset: str,
    timeout: float,
    retries: int,
) -> list[SwapBalanceRow]:
    response = signed_post(
        host=host,
        path="/linear-swap-api/v1/swap_balance_valuation",
        access_key=key.access_key,
        secret_key=key.secret_key,
        body={"valuation_asset": valuation_asset},
        timeout=timeout,
        retries=retries,
    )
    data = response.get("data", [])
    if not isinstance(data, list):
        raise HuobiApiError(f"Unexpected swap valuation response: {response}")

    rows: list[SwapBalanceRow] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        asset = str(item.get("valuation_asset", valuation_asset)).upper()
        rows.append(
            SwapBalanceRow(
                key_no=key.no,
                uid=key.uid,
                note=key.note,
                source="valuation",
                margin_mode="",
                margin_account=asset,
                margin_asset=asset,
                contract_code="",
                margin_balance=to_decimal(item.get("balance", "0")),
                margin_static=Decimal("0"),
                margin_available=Decimal("0"),
                margin_frozen=Decimal("0"),
                margin_position=Decimal("0"),
                profit_unreal=Decimal("0"),
                withdraw_available=Decimal("0"),
            )
        )
    return rows


def swap_row_from_item(key: ApiKey, source: str, item: dict[str, Any]) -> SwapBalanceRow:
    return SwapBalanceRow(
        key_no=key.no,
        uid=key.uid,
        note=key.note,
        source=source,
        margin_mode=str(item.get("margin_mode", "")),
        margin_account=str(item.get("margin_account", "")),
        margin_asset=str(item.get("margin_asset", "")),
        contract_code=str(item.get("contract_code", "")),
        margin_balance=to_decimal(item.get("margin_balance", "0")),
        margin_static=to_decimal(item.get("margin_static", "0")),
        margin_available=to_decimal(item.get("margin_available", "0")),
        margin_frozen=to_decimal(item.get("margin_frozen", "0")),
        margin_position=to_decimal(item.get("margin_position", "0")),
        profit_unreal=to_decimal(item.get("profit_unreal", "0")),
        withdraw_available=to_decimal(item.get("withdraw_available", "0")),
    )


def to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def query_all(args: argparse.Namespace, keys: Iterable[ApiKey]) -> QueryResult:
    spot_rows: list[BalanceRow] = []
    swap_rows: list[SwapBalanceRow] = []
    errors: list[str] = []

    selected_keys = select_keys(args, keys)
    for offset, key in enumerate(selected_keys, start=1):
        prefix = f"key no={key.no} uid={key.uid or '-'}"
        print(f"[{offset}/{len(selected_keys)}] querying {prefix} ...", file=sys.stderr)

        if args.market in {"spot", "all"}:
            rows, spot_errors = query_spot_key(args, key, prefix)
            spot_rows.extend(rows)
            errors.extend(spot_errors)

        if args.market in {"usdt-swap", "all"}:
            rows, swap_errors = query_usdt_swap_key(args, key, prefix)
            swap_rows.extend(rows)
            errors.extend(swap_errors)

        if args.delay > 0 and offset < len(selected_keys):
            time.sleep(args.delay)

    return QueryResult(spot_rows=spot_rows, swap_rows=swap_rows, errors=errors)


def select_keys(args: argparse.Namespace, keys: Iterable[ApiKey]) -> list[ApiKey]:
    nos = {str(no) for no in args.no or []}
    selected_keys = list(keys)
    if nos:
        selected_keys = [key for key in selected_keys if key.no in nos]
    if args.limit > 0:
        selected_keys = selected_keys[: args.limit]
    return selected_keys


def query_spot_key(
    args: argparse.Namespace,
    key: ApiKey,
    prefix: str,
) -> tuple[list[BalanceRow], list[str]]:
    rows: list[BalanceRow] = []
    errors: list[str] = []
    currencies = {c.lower() for c in args.currency or []}

    try:
        accounts = get_accounts(key, host=args.host, timeout=args.timeout, retries=args.retries)
        if args.account_type.lower() != "all":
            accounts = [
                account
                for account in accounts
                if str(account.get("type", "")).lower() == args.account_type.lower()
            ]

        for account in accounts:
            for row in get_balance(key, account, host=args.host, timeout=args.timeout, retries=args.retries):
                if currencies and row.currency not in currencies:
                    continue
                if not args.include_zero and row.total == 0:
                    continue
                rows.append(row)
    except Exception as exc:  # Keep processing other keys after one key fails.
        errors.append(f"{prefix} spot: {exc}")

    return rows, errors


def query_usdt_swap_key(
    args: argparse.Namespace,
    key: ApiKey,
    prefix: str,
) -> tuple[list[SwapBalanceRow], list[str]]:
    rows: list[SwapBalanceRow] = []
    errors: list[str] = []
    currencies = {c.lower() for c in args.currency or []}
    valuation_asset = (args.swap_margin_account or "USDT").upper()

    try:
        rows.extend(
            get_swap_valuation(
                key,
                host=args.swap_host,
                valuation_asset=valuation_asset,
                timeout=args.timeout,
                retries=args.retries,
            )
        )
    except Exception as exc:
        errors.append(f"{prefix} usdt-swap valuation: {exc}")

    if args.swap_margin_mode in {"cross", "all"}:
        try:
            rows.extend(
                get_swap_cross_balance(
                    key,
                    host=args.swap_host,
                    margin_account=args.swap_margin_account,
                    timeout=args.timeout,
                    retries=args.retries,
                )
            )
        except Exception as exc:
            errors.append(f"{prefix} usdt-swap cross: {exc}")

    if args.swap_margin_mode in {"isolated", "all"}:
        try:
            rows.extend(
                get_swap_isolated_balance(
                    key,
                    host=args.swap_host,
                    contract_code=args.contract_code,
                    timeout=args.timeout,
                    retries=args.retries,
                )
            )
        except Exception as exc:
            errors.append(f"{prefix} usdt-swap isolated: {exc}")

    filtered_rows = []
    for row in rows:
        if currencies and row.margin_asset.lower() not in currencies:
            continue
        if not args.include_zero and not swap_row_has_value(row):
            continue
        filtered_rows.append(row)
    return filtered_rows, errors


def swap_row_has_value(row: SwapBalanceRow) -> bool:
    return any(
        value != 0
        for value in [
            row.margin_balance,
            row.margin_static,
            row.margin_available,
            row.margin_frozen,
            row.margin_position,
            row.profit_unreal,
            row.withdraw_available,
        ]
    )


def print_spot_table(rows: list[BalanceRow]) -> None:
    print("\nSpot balances:")
    print_table(rows)


def print_swap_table(rows: list[SwapBalanceRow]) -> None:
    print("\nUSDT-margined swap balances:")
    headers = [
        "no",
        "uid",
        "source",
        "mode",
        "account",
        "asset",
        "contract",
        "balance",
        "available",
        "frozen",
        "position",
        "unreal_pnl",
        "withdraw",
        "note",
    ]
    table = [
        [
            row.key_no,
            row.uid,
            row.source,
            row.margin_mode,
            row.margin_account,
            row.margin_asset,
            row.contract_code,
            format_decimal(row.margin_balance),
            format_decimal(row.margin_available),
            format_decimal(row.margin_frozen),
            format_decimal(row.margin_position),
            format_decimal(row.profit_unreal),
            format_decimal(row.withdraw_available),
            row.note,
        ]
        for row in rows
    ]
    print_rendered_table(headers, table)


def print_rendered_table(headers: list[str], table: list[list[str]]) -> None:
    widths = [
        max(len(str(value)) for value in [header] + [line[index] for line in table])
        for index, header in enumerate(headers)
    ]
    print("  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("  ".join("-" * width for width in widths))
    for line in table:
        print("  ".join(str(value).ljust(widths[index]) for index, value in enumerate(line)))


def print_table(rows: list[BalanceRow]) -> None:
    headers = [
        "no",
        "uid",
        "account_id",
        "account_type",
        "currency",
        "trade",
        "frozen",
        "total",
        "note",
    ]
    table = [
        [
            row.key_no,
            row.uid,
            row.account_id,
            row.account_type,
            row.currency,
            format_decimal(row.trade),
            format_decimal(row.frozen),
            format_decimal(row.total),
            row.note,
        ]
        for row in rows
    ]

    print_rendered_table(headers, table)


def format_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f") if value else "0"


def write_csv(path: str, result: QueryResult) -> None:
    rows = result_to_dicts(result)
    with Path(path).open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=csv_headers())
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: str, result: QueryResult) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "spot_count": len(result.spot_rows),
        "usdt_swap_count": len(result.swap_rows),
        "rows": result_to_dicts(result),
        "errors": result.errors,
    }
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def result_to_dicts(result: QueryResult) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    rows.extend(spot_row_to_dict(row) for row in result.spot_rows)
    rows.extend(swap_row_to_dict(row) for row in result.swap_rows)
    return rows


def spot_row_to_dict(row: BalanceRow) -> dict[str, str]:
    return {
        "market": "spot",
        "no": row.key_no,
        "uid": row.uid,
        "note": row.note,
        "source": "account_balance",
        "account_id": row.account_id,
        "account_type": row.account_type,
        "account_state": row.account_state,
        "currency": row.currency,
        "trade": format_decimal(row.trade),
        "frozen": format_decimal(row.frozen),
        "total": format_decimal(row.total),
        "margin_mode": "",
        "margin_account": "",
        "margin_asset": "",
        "contract_code": "",
        "margin_balance": "",
        "margin_static": "",
        "margin_available": "",
        "margin_frozen": "",
        "margin_position": "",
        "profit_unreal": "",
        "withdraw_available": "",
    }


def swap_row_to_dict(row: SwapBalanceRow) -> dict[str, str]:
    return {
        "market": "usdt-swap",
        "no": row.key_no,
        "uid": row.uid,
        "note": row.note,
        "source": row.source,
        "account_id": "",
        "account_type": "",
        "account_state": "",
        "currency": row.margin_asset,
        "trade": "",
        "frozen": "",
        "total": "",
        "margin_mode": row.margin_mode,
        "margin_account": row.margin_account,
        "margin_asset": row.margin_asset,
        "contract_code": row.contract_code,
        "margin_balance": format_decimal(row.margin_balance),
        "margin_static": format_decimal(row.margin_static),
        "margin_available": format_decimal(row.margin_available),
        "margin_frozen": format_decimal(row.margin_frozen),
        "margin_position": format_decimal(row.margin_position),
        "profit_unreal": format_decimal(row.profit_unreal),
        "withdraw_available": format_decimal(row.withdraw_available),
    }


def csv_headers() -> list[str]:
    return [
        "market",
        "no",
        "uid",
        "note",
        "source",
        "account_id",
        "account_type",
        "account_state",
        "currency",
        "trade",
        "frozen",
        "total",
        "margin_mode",
        "margin_account",
        "margin_asset",
        "contract_code",
        "margin_balance",
        "margin_static",
        "margin_available",
        "margin_frozen",
        "margin_position",
        "profit_unreal",
        "withdraw_available",
    ]


def main() -> int:
    args = parse_args()
    try:
        keys = load_api_keys(args.keys)
        result = query_all(args, keys)
    except Exception as exc:
        print(f"fatal: {exc}", file=sys.stderr)
        return 1

    if result.spot_rows:
        print_spot_table(result.spot_rows)
    if result.swap_rows:
        print_swap_table(result.swap_rows)
    if not result.spot_rows and not result.swap_rows:
        print("No balances matched the filters.")

    if result.errors:
        print("\nErrors:", file=sys.stderr)
        for error in result.errors:
            print(f"  - {error}", file=sys.stderr)

    if args.csv_path:
        write_csv(args.csv_path, result)
        print(f"\nCSV written: {args.csv_path}", file=sys.stderr)
    if args.json_path:
        write_json(args.json_path, result)
        print(f"JSON written: {args.json_path}", file=sys.stderr)

    return 2 if result.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
