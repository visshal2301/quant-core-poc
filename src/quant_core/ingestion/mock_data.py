"""Mock data generation utilities for Quant Core."""

import argparse
import csv
import random
import shutil
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple


ASSET_CLASSES = ["EQUITY", "BOND", "ETF", "MUTUAL_FUND", "DERIVATIVE"]
CURRENCIES = ["USD", "EUR", "GBP", "INR"]
MARKET_DATA_SOURCE = "MOCK_MARKET_FEED"
STRESS_MONTHS = {"202409", "202410", "202411"}


@dataclass(frozen=True)
class MockDataConfig:
    base_dir: Path
    target_yyyymm: str
    portfolio_count: int = 10
    instrument_count: int = 50
    counterparty_count: int = 12
    replace_existing: bool = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Quant Core mock data for a target YYYYMM period.")
    parser.add_argument("--yyyymm", required=True, help="Target period in YYYYMM format, for example 202604.")
    parser.add_argument(
        "--base-dir",
        default="/Volumes/quant_core/landing/mock_data",
        help="Base landing directory. Defaults to the Databricks volume path.",
    )
    parser.add_argument(
        "--keep-existing",
        action="store_true",
        help="Keep an existing YYYYMM folder instead of deleting and recreating it.",
    )
    return parser.parse_args()


def validate_yyyymm(yyyymm: str) -> None:
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        raise ValueError("yyyymm must be in YYYYMM format.")
    year_num = int(yyyymm[:4])
    month_num = int(yyyymm[4:6])
    if year_num < 2000 or month_num < 1 or month_num > 12:
        raise ValueError("yyyymm is out of supported range.")


def month_bounds(yyyymm: str) -> Tuple[date, date]:
    year_num = int(yyyymm[:4])
    month_num = int(yyyymm[4:6])
    start_dt = date(year_num, month_num, 1)
    if month_num == 12:
        next_month = date(year_num + 1, 1, 1)
    else:
        next_month = date(year_num, month_num + 1, 1)
    return start_dt, next_month - timedelta(days=1)


def daterange(start_dt: date, end_dt: date):
    current = start_dt
    while current <= end_dt:
        yield current
        current += timedelta(days=1)


def ensure_dirs(config: MockDataConfig) -> Dict[str, Path]:
    month_dir = config.target_yyyymm
    folders = {
        "dimensions": config.base_dir / "dimensions" / month_dir,
        "transactions": config.base_dir / "transactions" / month_dir,
        "positions_daily": config.base_dir / "positions_daily" / month_dir,
        "market_prices_daily": config.base_dir / "market_prices_daily" / month_dir,
        "cashflows": config.base_dir / "cashflows" / month_dir,
    }

    for folder in folders.values():
        if folder.exists() and config.replace_existing:
            shutil.rmtree(str(folder))
        folder.mkdir(parents=True, exist_ok=True)
    return folders


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_rows_by_day(folder: Path, prefix: str, rows: List[dict], date_key: str) -> int:
    rows_by_day = {}  # type: Dict[str, List[dict]]
    for row in rows:
        day_key = str(row[date_key]).replace("-", "")
        rows_by_day.setdefault(day_key, []).append(row)

    for day_key, day_rows in rows_by_day.items():
        write_csv(folder / "{0}_{1}.csv".format(prefix, day_key), day_rows, list(day_rows[0].keys()))
    return len(rows_by_day)


def generate_dimensions(config: MockDataConfig, folders: Dict[str, Path], end_dt: date) -> None:
    portfolios = []
    for idx in range(1, config.portfolio_count + 1):
        portfolios.append(
            {
                "portfolio_id": "PORT{0:03d}".format(idx),
                "portfolio_name": "Portfolio {0}".format(idx),
                "portfolio_type": random.choice(["BALANCED", "GROWTH", "INCOME"]),
                "base_currency_code": random.choice(CURRENCIES),
                "risk_policy_name": random.choice(["STANDARD", "CONSERVATIVE", "AGGRESSIVE"]),
                "as_of_month": config.target_yyyymm,
            }
        )

    instruments = []
    for idx in range(1, config.instrument_count + 1):
        asset_class = random.choice(ASSET_CLASSES)
        instruments.append(
            {
                "instrument_id": "INS{0:04d}".format(idx),
                "instrument_name": "Instrument {0}".format(idx),
                "ticker": "TCK{0:04d}".format(idx),
                "isin": "ISIN{0:08d}".format(idx),
                "asset_class_code": asset_class,
                "currency_code": random.choice(CURRENCIES),
                "issuer_name": "Issuer {0}".format(random.randint(1, 15)),
                "coupon_rate": round(random.uniform(0.01, 0.08), 4) if asset_class == "BOND" else 0.0,
                "maturity_dt": end_dt + timedelta(days=random.randint(365, 3650)),
                "as_of_month": config.target_yyyymm,
            }
        )

    counterparties = []
    for idx in range(1, config.counterparty_count + 1):
        counterparties.append(
            {
                "counterparty_id": "CP{0:03d}".format(idx),
                "counterparty_name": "Counterparty {0}".format(idx),
                "counterparty_type": random.choice(["BROKER", "CUSTODIAN", "BANK"]),
                "country_code": random.choice(["US", "GB", "IN", "DE"]),
                "credit_rating": random.choice(["AAA", "AA", "A", "BBB"]),
                "as_of_month": config.target_yyyymm,
            }
        )

    currencies = [
        {"currency_code": code, "currency_name": code, "decimal_precision": 2, "as_of_month": config.target_yyyymm}
        for code in CURRENCIES
    ]
    asset_classes = [
        {"asset_class_code": code, "asset_class_name": code.title(), "risk_bucket": code, "as_of_month": config.target_yyyymm}
        for code in ASSET_CLASSES
    ]
    market_data_sources = [
        {
            "source_system_code": "MOCK_FEED",
            "source_system_name": "Mock Feed",
            "vendor_name": MARKET_DATA_SOURCE,
            "as_of_month": config.target_yyyymm,
        }
    ]

    write_csv(folders["dimensions"] / "portfolios.csv", portfolios, list(portfolios[0].keys()))
    write_csv(folders["dimensions"] / "instruments.csv", instruments, list(instruments[0].keys()))
    write_csv(folders["dimensions"] / "counterparties.csv", counterparties, list(counterparties[0].keys()))
    write_csv(folders["dimensions"] / "currencies.csv", currencies, list(currencies[0].keys()))
    write_csv(folders["dimensions"] / "asset_classes.csv", asset_classes, list(asset_classes[0].keys()))
    write_csv(folders["dimensions"] / "market_data_sources.csv", market_data_sources, list(market_data_sources[0].keys()))


def generate_market_prices(config: MockDataConfig, folders: Dict[str, Path], start_dt: date, end_dt: date) -> List[dict]:
    rows = []
    stressed_month = config.target_yyyymm in STRESS_MONTHS
    for instrument_idx in range(1, config.instrument_count + 1):
        price = random.uniform(40, 180)
        for price_dt in daterange(start_dt, end_dt):
            daily_shock = random.gauss(0.0004, 0.012)
            if stressed_month:
                daily_shock += random.gauss(-0.015, 0.035)
            price = max(1.0, price * (1 + daily_shock))
            rows.append(
                {
                    "price_id": "PRC{0:04d}{1}".format(instrument_idx, price_dt.strftime("%Y%m%d")),
                    "instrument_id": "INS{0:04d}".format(instrument_idx),
                    "price_dt": price_dt.isoformat(),
                    "close_price": round(price, 4),
                    "return_pct": round(daily_shock, 6),
                    "volatility_proxy": round(abs(daily_shock) * 100, 4),
                    "currency_code": random.choice(CURRENCIES),
                    "source_system_code": "MOCK_FEED",
                    "as_of_month": config.target_yyyymm,
                }
            )
    write_rows_by_day(folders["market_prices_daily"], "market_prices_daily", rows, "price_dt")
    return rows


def generate_transactions_positions_cashflows(
    config: MockDataConfig,
    folders: Dict[str, Path],
    market_prices: List[dict],
    start_dt: date,
    end_dt: date,
) -> Tuple[int, int, int]:
    transactions = []
    positions = []
    cashflows = []
    price_lookup = {}  # type: Dict[Tuple[str, str], float]
    for row in market_prices:
        price_lookup[(row["instrument_id"], row["price_dt"])] = float(row["close_price"])

    for portfolio_idx in range(1, config.portfolio_count + 1):
        all_days = list(daterange(start_dt, end_dt))
        transaction_days = random.sample(all_days, k=min(20, len(all_days)))
        for txn_idx, txn_dt in enumerate(sorted(transaction_days), start=1):
            instrument_id = "INS{0:04d}".format(random.randint(1, config.instrument_count))
            quantity = random.randint(10, 500)
            price = price_lookup.get((instrument_id, txn_dt.isoformat()), random.uniform(25, 150))
            gross = round(quantity * price, 2)
            fees = round(gross * random.uniform(0.0005, 0.0030), 2)
            transactions.append(
                {
                    "transaction_id": "TXN{0:03d}{1:05d}".format(portfolio_idx, txn_idx),
                    "portfolio_id": "PORT{0:03d}".format(portfolio_idx),
                    "instrument_id": instrument_id,
                    "counterparty_id": "CP{0:03d}".format(random.randint(1, config.counterparty_count)),
                    "trade_dt": txn_dt.isoformat(),
                    "settlement_dt": (txn_dt + timedelta(days=2)).isoformat(),
                    "transaction_type": random.choice(["BUY", "SELL"]),
                    "quantity": quantity,
                    "price": round(price, 4),
                    "gross_amount": gross,
                    "fees_amount": fees,
                    "net_amount": round(gross - fees, 2),
                    "currency_code": random.choice(CURRENCIES),
                    "as_of_month": config.target_yyyymm,
                }
            )

        for position_dt in daterange(start_dt, end_dt):
            for holding_idx in range(1, 11):
                instrument_id = "INS{0:04d}".format(((portfolio_idx - 1) * 3 + holding_idx) % config.instrument_count + 1)
                quantity = random.randint(100, 5000)
                close_price = price_lookup.get((instrument_id, position_dt.isoformat()), random.uniform(25, 150))
                market_value = round(quantity * close_price, 2)
                positions.append(
                    {
                        "position_id": "POS{0:03d}{1:03d}{2}".format(portfolio_idx, holding_idx, position_dt.strftime("%Y%m%d")),
                        "portfolio_id": "PORT{0:03d}".format(portfolio_idx),
                        "instrument_id": instrument_id,
                        "position_dt": position_dt.isoformat(),
                        "quantity": quantity,
                        "end_of_day_price": round(close_price, 4),
                        "market_value": market_value,
                        "unrealized_pnl": round(market_value * random.uniform(-0.03, 0.04), 2),
                        "currency_code": random.choice(CURRENCIES),
                        "source_system_code": "MOCK_FEED",
                        "as_of_month": config.target_yyyymm,
                    }
                )

        cashflows.append(
            {
                "cashflow_id": "CF{0:03d}00001".format(portfolio_idx),
                "portfolio_id": "PORT{0:03d}".format(portfolio_idx),
                "instrument_id": "",
                "cashflow_dt": start_dt.isoformat(),
                "cashflow_type": "INITIAL_INVESTMENT",
                "cashflow_amount": round(-1 * random.uniform(200000, 500000), 2),
                "currency_code": "USD",
                "as_of_month": config.target_yyyymm,
            }
        )
        for flow_idx in range(2, 5):
            flow_dt = start_dt + timedelta(days=random.randint(0, max(0, (end_dt - start_dt).days)))
            cashflows.append(
                {
                    "cashflow_id": "CF{0:03d}{1:05d}".format(portfolio_idx, flow_idx),
                    "portfolio_id": "PORT{0:03d}".format(portfolio_idx),
                    "instrument_id": "",
                    "cashflow_dt": flow_dt.isoformat(),
                    "cashflow_type": random.choice(["DIVIDEND", "COUPON", "CAPITAL_CALL", "DISTRIBUTION"]),
                    "cashflow_amount": round(random.uniform(-40000, 90000), 2),
                    "currency_code": "USD",
                    "as_of_month": config.target_yyyymm,
                }
            )

    transaction_file_count = write_rows_by_day(folders["transactions"], "transactions", transactions, "trade_dt")
    position_file_count = write_rows_by_day(folders["positions_daily"], "positions_daily", positions, "position_dt")
    cashflow_file_count = write_rows_by_day(folders["cashflows"], "cashflows", cashflows, "cashflow_dt")
    return transaction_file_count, position_file_count, cashflow_file_count


def build_config(args: argparse.Namespace) -> MockDataConfig:
    validate_yyyymm(args.yyyymm)
    return MockDataConfig(
        base_dir=Path(args.base_dir),
        target_yyyymm=args.yyyymm,
        replace_existing=not args.keep_existing,
    )


def generate_mock_data(config: MockDataConfig) -> dict:
    start_dt, end_dt = month_bounds(config.target_yyyymm)
    random.seed(42)
    folders = ensure_dirs(config)
    generate_dimensions(config, folders, end_dt)
    market_prices = generate_market_prices(config, folders, start_dt, end_dt)
    transaction_files, position_files, cashflow_files = generate_transactions_positions_cashflows(
        config, folders, market_prices, start_dt, end_dt
    )
    return {
        "base_dir": str(config.base_dir),
        "target_yyyymm": config.target_yyyymm,
        "dimensions_dir": str(folders["dimensions"]),
        "market_price_daily_files": len(set(row["price_dt"] for row in market_prices)),
        "transaction_daily_files": transaction_files,
        "position_daily_files": position_files,
        "cashflow_daily_files": cashflow_files,
    }


def main() -> None:
    args = parse_args()
    config = build_config(args)
    summary = generate_mock_data(config)
    print("Generating Quant Core mock data for {0}...".format(config.target_yyyymm))
    print("")
    print("Mock data generated successfully.")
    print("Base location: {0}".format(summary["base_dir"]))
    print("Target month: {0}".format(summary["target_yyyymm"]))
    print("Replace existing month folder: {0}".format(config.replace_existing))
    print("Dimensions folder: {0}".format(summary["dimensions_dir"]))
    print("Market price daily files: {0}".format(summary["market_price_daily_files"]))
    print("Transaction daily files: {0}".format(summary["transaction_daily_files"]))
    print("Position daily files: {0}".format(summary["position_daily_files"]))
    print("Cashflow daily files: {0}".format(summary["cashflow_daily_files"]))

