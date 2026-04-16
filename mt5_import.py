#!/usr/bin/env python3
"""
MT5 Backtest Report Importer
Parses an MT5 Strategy Tester xlsx export and inserts into a SQLite database.

Usage:
    python mt5_import.py <report.xlsx> <database.db> [--bot-id 1] [--bot-name "MyEA"]
"""

import argparse
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


# ──────────────────────────────────────────────
# Parsing helpers
# ──────────────────────────────────────────────

def parse_float(val):
    """Convert MT5 formatted numbers like '15 987.38' or '-8,030.71' to float."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).replace(" ", "").replace(",", "")
    # Strip trailing % sign
    s = s.rstrip("%")
    try:
        return float(s)
    except ValueError:
        return None


def parse_pct_from_string(val):
    """Extract percentage from strings like '14.88% (15 987.38)' or '15 987.38 (14.88%)'."""
    if val is None:
        return None
    s = str(val)
    match = re.search(r"([\d\s,.]+)%", s)
    if match:
        return parse_float(match.group(1))
    return None


def parse_date_range(period_str):
    """Parse 'M15 (2022.01.02 - 2026.03.28)' into (timeframe, date_from, date_to)."""
    if not period_str:
        return None, None, None
    s = str(period_str).strip()
    # Timeframe is the first token before the space/parenthesis
    tf_match = re.match(r"^(\S+)", s)
    timeframe = tf_match.group(1) if tf_match else None
    # Date range inside parentheses
    range_match = re.search(r"\((\d{4}\.\d{2}\.\d{2})\s*-\s*(\d{4}\.\d{2}\.\d{2})\)", s)
    if range_match:
        return timeframe, range_match.group(1), range_match.group(2)
    return timeframe, None, None


# ──────────────────────────────────────────────
# Section finders
# ──────────────────────────────────────────────

def find_row(df, label, col=0):
    """Return the first row index where df[col] == label (stripped)."""
    for i, row in df.iterrows():
        if str(row[col]).strip() == label:
            return i
    return None


def cell(df, row, col):
    """Safe cell accessor — returns None for NaN."""
    val = df.iloc[row][col]
    if pd.isna(val):
        return None
    return val


# ──────────────────────────────────────────────
# Section parsers
# ──────────────────────────────────────────────

def parse_settings(df):
    """Extract bot name, symbol, timeframe, dates, deposit, leverage."""
    info = {}

    for i, row in df.iterrows():
        label = str(row[0]).strip()
        value = row[3] if not pd.isna(row[3]) else None

        if label == "Expert:":
            info["bot_name"] = str(value).strip() if value else None
        elif label == "Symbol:":
            info["symbol"] = str(value).strip() if value else None
        elif label == "Period:":
            tf, d_from, d_to = parse_date_range(value)
            info["timeframe"] = tf
            info["test_from"] = d_from
            info["test_to"] = d_to
        elif label == "Company:":
            info["company"] = str(value).strip() if value else None
        elif label == "Initial Deposit:":
            info["initial_balance"] = parse_float(value)
        elif label == "Results":
            break  # Stop before results section

    return info


def parse_parameters(df):
    """Extract all EA input parameters from the Inputs: block."""
    params = {}
    in_inputs = False

    for i, row in df.iterrows():
        label = str(row[0]).strip()
        value = str(row[3]).strip() if not pd.isna(row[3]) else ""

        if label == "Inputs:":
            in_inputs = True
            # The first input is on the same row as "Inputs:"
            if value and "=" in value and not value.startswith("<"):
                k, v = value.split("=", 1)
                params[k.strip()] = v.strip()
            continue

        if in_inputs:
            # Stop when we hit a new labelled section
            if label and label not in ["", "nan"] and not pd.isna(row[0]):
                if label in ("Company:", "Currency:", "Initial Deposit:", "Leverage:", "Results"):
                    in_inputs = False
                    break
            if value and "=" in value and not value.startswith("<"):
                k, v = value.split("=", 1)
                params[k.strip()] = v.strip()

    return params


def parse_results(df):
    """Extract performance metrics from the Results section."""
    results = {}
    results_row = find_row(df, "Results")
    if results_row is None:
        return results

    # Scan rows in the results block until Orders section
    orders_row = find_row(df, "Orders") or len(df)

    for i in range(results_row, orders_row):
        row = df.iloc[i]

        def r(c): return None if pd.isna(row[c]) else row[c]

        label0 = str(r(0) or "").strip()
        label4 = str(r(4) or "").strip()
        label8 = str(r(8) or "").strip()

        # col 0 / col 3 pairs
        if label0 == "Bars:":
            results["bars"] = int(r(3)) if r(3) else None
            results["ticks"] = int(r(7)) if r(7) else None
        elif label0 == "Total Net Profit:":
            results["net_profit"] = parse_float(r(3))
        elif label0 == "Gross Profit:":
            results["gross_profit"] = parse_float(r(3))
        elif label0 == "Gross Loss:":
            results["gross_loss"] = parse_float(r(3))
        elif label0 == "Profit Factor:":
            results["profit_factor"] = parse_float(r(3))
        elif label0 == "Recovery Factor:":
            results["recovery_factor"] = parse_float(r(3))
        elif label0 == "Total Trades:":
            results["total_trades"] = int(r(3)) if r(3) else None
        elif label0 == "AHPR:":
            pass  # not in schema

        # col 4 / col 7 pairs
        if label4 == "Expected Payoff:":
            results["expected_payoff"] = parse_float(r(7))
        elif label4 == "Sharpe Ratio:":
            results["sharpe_ratio"] = parse_float(r(7))
        elif label4 == "Balance Drawdown Absolute:":
            results["drawdown_abs"] = parse_float(r(7))
        elif label4 == "Balance Drawdown Maximal:":
            # e.g. "15 987.38 (14.88%)"
            results["drawdown_pct"] = parse_pct_from_string(r(7))
        elif label4 == "Profit Trades (% of total):":
            # e.g. "120 (38.46%)"  → extract win count and pct
            val = str(r(7) or "")
            count_match = re.match(r"(\d+)", val)
            pct_match = re.search(r"\(([\d.]+)%\)", val)
            if count_match:
                results["_win_trades"] = int(count_match.group(1))
            if pct_match:
                results["win_rate"] = float(pct_match.group(1))

        # Model is in the Settings-ish area; handle separately below

    return results


def parse_model(df):
    """MT5 doesn't always export the model type explicitly, but we can note it."""
    # Look for a "Model:" label
    for i, row in df.iterrows():
        if str(row[0]).strip() == "Model:":
            val = row[3]
            return str(val).strip() if not pd.isna(val) else None
    return "Every tick"  # MT5 default


def parse_deals(df):
    """Parse the Deals section into a list of dicts."""
    deals_row = find_row(df, "Deals")
    if deals_row is None:
        return []

    header_row = deals_row + 1
    # Expected: Time, Deal, Symbol, Type, Direction, Volume, Price, Order, Commission, Swap, Profit, Balance, Comment
    data_start = header_row + 1

    deals = []
    for i in range(data_start, len(df)):
        row = df.iloc[i]

        # Stop on empty time or totals row (last row has NaN time)
        time_val = row[0]
        if pd.isna(time_val):
            break

        direction = str(row[4] or "").strip().lower()
        # Skip the opening balance entry and opening legs (direction="in")
        # We only want closed trades (direction="out") to match your trades table
        # Alternatively keep all deals — up to you. Here we keep ALL including "in".
        deal_type = str(row[3] or "").strip().lower()
        if deal_type == "balance":
            continue  # skip the initial deposit entry

        deals.append({
            "ticket": str(int(row[1])) if not pd.isna(row[1]) else None,
            "time": str(time_val),
            "symbol": str(row[2]).strip() if not pd.isna(row[2]) else None,
            "type": deal_type,          # buy / sell
            "direction": direction,     # in / out
            "volume": parse_float(row[5]),
            "price": parse_float(row[6]),
            "order": str(int(row[7])) if not pd.isna(row[7]) else None,
            "commission": parse_float(row[8]),
            "swap": parse_float(row[9]),
            "profit": parse_float(row[10]),
            "balance": parse_float(row[11]),
            "comment": str(row[12]).strip() if not pd.isna(row[12]) else None,
        })

    return deals


def pair_deals_to_trades(deals):
    """
    Match opening (in) and closing (out) deals into complete trades.

    MT5 Deals section: in this EA the Deal# == Order# for every deal, so the
    Order field on the out-leg does NOT point back to the in-leg's Deal#.
    Instead the EA opens N legs in a burst, then closes them in the same order.
    We use a FIFO queue keyed by (symbol, entry_type) to pair them correctly.

    Returns list of trade dicts matching your trades table schema.
    """
    from collections import deque
    open_queues = {}   # (symbol, entry_direction) -> deque of open deal dicts
    trades = []

    for d in deals:
        symbol = d["symbol"] or ""
        # entry direction = the 'type' of the in-leg (buy/sell)
        entry_direction = d["type"]

        if d["direction"] == "in":
            key = (symbol, entry_direction)
            if key not in open_queues:
                open_queues[key] = deque()
            open_queues[key].append(d)

        elif d["direction"] == "out":
            # The out-leg type is opposite to entry (sell closes buy, buy closes sell)
            entry_direction = "buy" if d["type"] == "sell" else "sell"
            key = (symbol, entry_direction)
            open_deal = None
            if key in open_queues and open_queues[key]:
                open_deal = open_queues[key].popleft()
            else:
                open_deal = {}
            if open_deal is None:
                # Fallback: no match, store as-is with partial info
                open_deal = {}

            open_time = open_deal.get("time")
            close_time = d["time"]

            # Duration in minutes
            duration = None
            if open_time and close_time:
                try:
                    fmt = "%Y.%m.%d %H:%M:%S"
                    t_open = datetime.strptime(open_time, fmt)
                    t_close = datetime.strptime(close_time, fmt)
                    duration = int((t_close - t_open).total_seconds() / 60)
                except ValueError:
                    pass

            # Determine weekday from open time
            weekday = None
            if open_time:
                try:
                    weekday = datetime.strptime(open_time, "%Y.%m.%d %H:%M:%S").strftime("%A")
                except ValueError:
                    pass

            direction = open_deal.get("type") or d["type"]  # buy/sell based on entry

            trades.append({
                "ticket": d["order"],       # use the order number as ticket
                "open_time": open_time,
                "close_time": close_time,
                "symbol": d["symbol"],
                "direction": direction,
                "volume": open_deal.get("volume"),
                "entry_price": open_deal.get("price"),
                "exit_price": d["price"],
                "stop_loss": None,          # not in Deals section; Orders section has it
                "take_profit": None,
                "commission": (open_deal.get("commission") or 0) + (d.get("commission") or 0),
                "swap": (open_deal.get("swap") or 0) + (d.get("swap") or 0),
                "profit": d["profit"],
                "duration_minutes": duration,
                "weekday": weekday,
                "session": None,            # can be derived from open_time if needed
            })

    return trades


# ──────────────────────────────────────────────
# Database helpers
# ──────────────────────────────────────────────

def get_or_create_bot(conn, bot_name, version=None):
    """Return existing bot_id or insert a new bot and return its id."""
    cur = conn.execute(
        "SELECT bot_id FROM bots WHERE bot_name = ? AND (version = ? OR (version IS NULL AND ? IS NULL))",
        (bot_name, version, version)
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO bots (bot_name, version) VALUES (?, ?)",
        (bot_name, version)
    )
    conn.commit()
    return cur.lastrowid


def insert_run(conn, bot_id, source_file, settings, results):
    cur = conn.execute("""
        INSERT INTO runs (
            bot_id, run_date, source_file, symbol, timeframe, model,
            test_from, test_to, initial_balance,
            net_profit, gross_profit, gross_loss, profit_factor,
            expected_payoff, recovery_factor, sharpe_ratio,
            drawdown_abs, drawdown_pct,
            total_trades, win_rate, bars, ticks
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        bot_id,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        source_file,
        settings.get("symbol"),
        settings.get("timeframe"),
        settings.get("model"),
        settings.get("test_from"),
        settings.get("test_to"),
        settings.get("initial_balance"),
        results.get("net_profit"),
        results.get("gross_profit"),
        results.get("gross_loss"),
        results.get("profit_factor"),
        results.get("expected_payoff"),
        results.get("recovery_factor"),
        results.get("sharpe_ratio"),
        results.get("drawdown_abs"),
        results.get("drawdown_pct"),
        results.get("total_trades"),
        results.get("win_rate"),
        results.get("bars"),
        results.get("ticks"),
    ))
    conn.commit()
    return cur.lastrowid


def insert_parameters(conn, run_id, params):
    rows = [(run_id, k, v) for k, v in params.items()]
    conn.executemany(
        "INSERT INTO parameters (run_id, parameter_name, parameter_value) VALUES (?,?,?)",
        rows
    )
    conn.commit()


def insert_trades(conn, run_id, trades):
    rows = [(
        run_id,
        t["ticket"],
        t["open_time"],
        t["close_time"],
        t["symbol"],
        t["direction"],
        t["volume"],
        t["entry_price"],
        t["exit_price"],
        t["stop_loss"],
        t["take_profit"],
        t["commission"],
        t["swap"],
        t["profit"],
        t["duration_minutes"],
        t["weekday"],
        t["session"],
    ) for t in trades]

    conn.executemany("""
        INSERT INTO trades (
            run_id, ticket, open_time, close_time, symbol, direction,
            volume, entry_price, exit_price, stop_loss, take_profit,
            commission, swap, profit, duration_minutes, weekday, session
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def import_report(xlsx_path: str, db_path: str, bot_id: int = None, bot_name: str = None, version: str = None):
    print(f"📂 Reading: {xlsx_path}")
    df = pd.read_excel(xlsx_path, sheet_name=0, header=None)
    print(f"   {len(df)} rows loaded")

    # Parse sections
    settings = parse_settings(df)
    settings["model"] = parse_model(df)
    params = parse_parameters(df)
    results = parse_results(df)
    deals = parse_deals(df)
    trades = pair_deals_to_trades(deals)

    # Resolve bot name
    resolved_bot_name = bot_name or settings.get("bot_name") or "Unknown"
    source_file = Path(xlsx_path).name

    print(f"\n🤖 EA:         {resolved_bot_name}")
    print(f"📈 Symbol:     {settings.get('symbol')}  {settings.get('timeframe')}")
    print(f"📅 Period:     {settings.get('test_from')} → {settings.get('test_to')}")
    print(f"💰 Balance:    {settings.get('initial_balance')}")
    print(f"📊 Net Profit: {results.get('net_profit')}")
    print(f"🏆 Win Rate:   {results.get('win_rate')}%")
    print(f"🔢 Trades:     {len(trades)} (paired from {len(deals)} deals)")
    print(f"⚙️  Parameters: {len(params)}")

    # Connect to DB
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    # Get or create bot
    if bot_id is None:
        bot_id = get_or_create_bot(conn, resolved_bot_name, version)
        print(f"\n🗄️  bot_id: {bot_id} ({'existing' if bot_id else 'new'})")

    # Insert run
    run_id = insert_run(conn, bot_id, source_file, settings, results)
    print(f"✅ run_id: {run_id} inserted")

    # Insert parameters
    insert_parameters(conn, run_id, params)
    print(f"✅ {len(params)} parameters inserted")

    # Insert trades
    insert_trades(conn, run_id, trades)
    print(f"✅ {len(trades)} trades inserted")

    conn.close()
    print(f"\n🎉 Done! run_id={run_id} in {db_path}")
    return run_id


def main():
    parser = argparse.ArgumentParser(description="Import MT5 backtest xlsx into SQLite")
    parser.add_argument("xlsx", help="Path to MT5 xlsx report")
    parser.add_argument("db", help="Path to SQLite database file")
    parser.add_argument("--bot-id", type=int, default=None,
                        help="Existing bot_id to link run to (skips auto-create)")
    parser.add_argument("--bot-name", default=None,
                        help="Override bot name (default: read from xlsx)")
    parser.add_argument("--version", default=None,
                        help="Bot version tag (e.g. 'v4.1')")
    args = parser.parse_args()

    if not Path(args.xlsx).exists():
        print(f"❌ File not found: {args.xlsx}", file=sys.stderr)
        sys.exit(1)
    if not Path(args.db).exists():
        print(f"❌ Database not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    import_report(args.xlsx, args.db, args.bot_id, args.bot_name, args.version)


if __name__ == "__main__":
    main()
