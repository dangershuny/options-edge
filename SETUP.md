# Options Edge Scanner — Setup

## First-time setup

```bash
cd C:/Users/dange/Personal_Projects/options-edge

# Create a virtual environment (recommended)
python -m venv venv
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Running the app

```bash
# Make sure venv is active
venv\Scripts\activate

streamlit run app.py
```

The browser will open automatically at http://localhost:8501

## How to use

1. Add tickers to your watchlist in the sidebar
2. Click **Scan All** or select specific tickers and click **Scan Selected**
3. Results are sorted by **Score** — higher = stronger edge signal
4. Use the filters (min score, vol signal, type) to narrow down
5. Check the **Recent News** section below the table to cross-reference signals with what the market may or may not have priced in

## Signal meanings

| Signal | Meaning | Action |
|--------|---------|--------|
| BUY VOL | IV is >20% below 30-day realized vol — options are cheap | Consider buying calls or puts |
| SELL VOL | IV is >25% above realized vol — options are expensive | Consider credit spreads (defined risk only) |
| NEUTRAL | IV and RV are roughly in line | Watch only |
| STRONG flow | Volume/OI ratio ≥ 1× — someone is building a big position | Directional clue |
| ELEVATED flow | Volume/OI ratio ≥ 0.3× | Minor clue |

## What is automatically excluded

- Contracts within 10 days of an earnings date
- Contracts more than 10% out of the money
- Contracts with fewer than 25 trades today
- Contracts expiring in fewer than 7 days or more than 90 days
