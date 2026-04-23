# Huobi Daily Balance Report

查询 HTX/Huobi U 本位合约账户总资产和开单/持仓情况，并通过企业微信机器人 Webhook 发送报告。

报告取的是 U 本位合约接口 `source=valuation` 对应的 `balance`，即账户总资产估值。
开单情况取当前 U 本位合约持仓，包含合约、方向、张数、持仓均价、最新价和未实现盈亏。

## 本地运行

```bash
python3 huobi_wecom_valuation_report.py --dry-run
```

发送到企业微信：

```bash
export WECOM_WEBHOOK_URL='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...'
python3 huobi_wecom_valuation_report.py
```

只查某个编号：

```bash
python3 huobi_wecom_valuation_report.py --no 520 --dry-run
```

## GitHub Actions Secrets

仓库里不要提交 API Key JSON。请在 GitHub 仓库 Settings -> Secrets and variables -> Actions 里添加：

- `HUOBI_KEYS_JSON`：完整的授权 JSON 内容。
- `WECOM_WEBHOOK_URL`：企业微信机器人 Webhook URL。

## 定时任务

GitHub Actions 使用 UTC cron。北京时间是 UTC+8，所以每天北京时间 10:00 和 18:00 对应 UTC 02:00 和 10:00。
