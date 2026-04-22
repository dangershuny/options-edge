# Alpaca Paper Trading Setup

To execute paper trades through the tool, you need an Alpaca paper account
and API keys. Paper trading is **free** and doesn't need a funded brokerage.

## Step 1 — Sign up

1. Go to https://alpaca.markets
2. Click **"Sign up for free"**
3. Complete basic signup (email, password)
4. **You do NOT need to fund an account** for paper trading

## Step 2 — Apply for options Level 2

1. After signup, go to https://app.alpaca.markets/brokerage/onboarding
2. Request **Options Level 2** (long calls + puts)
3. Usually auto-approved within minutes for paper

## Step 3 — Generate paper trading API keys

1. Go to https://app.alpaca.markets/paper/dashboard/overview
2. Click **"View"** next to "Your API Keys"
3. Copy both:
   - **API Key ID** (starts with `PK...`, ~20 chars)
   - **Secret Key** (longer, ~40+ chars)
4. **Save these — the secret is shown only once**

## Step 4 — Set environment variables

Open **Command Prompt (cmd.exe)** and run:

```cmd
setx ALPACA_API_KEY "PKxxxxxxxxxxxxxxxxxx"
setx ALPACA_API_SECRET "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
setx ALPACA_PAPER "true"
```

These persist across reboots. Close that terminal after — scheduled
tasks and new terminals will pick them up automatically.

## Step 5 — Verify

Open a **NEW** terminal (important — `setx` doesn't affect the current session):

```cmd
cd C:\Users\dange\Personal_Projects\options-edge-new
python -m broker.alpaca
```

You should see account details printed. If you see `BrokerError: ALPACA_API_KEY
and ALPACA_API_SECRET must be set`, the env vars didn't take — try reboot.

## Step 6 — First paper trade

Dry run (shows what would happen, submits nothing):

```cmd
python -m tools.paper_trade --bankroll 500
```

For real (submits to Alpaca paper — no real money):

```cmd
python -m tools.paper_trade --bankroll 500 --live
```

## Safety — Built-in Guardrails

- Tool **refuses to run** if `ALPACA_PAPER` is not `true` or missing
- Defaults to **dry run** — must pass `--live` to submit
- Per-trade cap: 15% of bankroll (configurable)
- Max trades per session: 5 (configurable)
- Only buys long options (no naked selling ever)

## Troubleshooting

**"ALPACA_API_KEY and ALPACA_API_SECRET must be set"**
→ Open a brand-new terminal. `setx` only affects future sessions.

**"401 Unauthorized"**
→ Keys are wrong. Regenerate at paper dashboard and re-run `setx`.

**"forbidden: options not enabled"**
→ Options Level 2 not approved yet. Check onboarding page.

**"insufficient buying power"**
→ Paper account starts with $100K virtual cash. If you hit this on a real
paper account, something is off — you probably hit max position value.

## Keys vs Secrets — Security

- Paper keys **cannot move real money** — they only work on paper accounts
- Still, treat them like passwords — don't commit to git, don't share
- The tool reads from environment variables only, never from a file in the repo
