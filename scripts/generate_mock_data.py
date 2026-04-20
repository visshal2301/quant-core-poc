import csv
import math
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path


@dataclass(frozen=True)
class Config:
    base_dir: Path
    prices_years: int = 3
    transactions_years: int = 1
    portfolio_count: int = 10
    instrument_count: int = 50
    counterparty_count: int = 12


ASSET_CLASSES = ["EQUITY", "BOND", "ETF", "MUTUAL_FUND", "DERIVATIVE"]
CURRENCIES = ["USD", "EUR", "GBP", "INR"]
MARKET_DATA_SOURCE = "MOCK_MARKET_FEED"
STRESS_START = date(2024, 9, 1)
STRESS_END = date(2024, 11, 30)


def daterange(start_dt: date, end_dt: date):
    current = start_dt
    while current <= end_dt:
        yield current
        current += timedelta(days=1)


def ensure_dirs(base_dir: Path) -> dict[str, Path]:
    folders = {
        "dimensions": base_dir / "dimensions",
        "transactions": base_dir / "transactions",
        "positions_daily": base_dir / "positions_daily",
        "market_prices_daily": base_dir / "market_prices_daily",
        "cashflows": base_dir / "cashflows",
    }
    for folder in folders.values():
        folder.mkdir(parents=True, exist_ok=True)
    return folders


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_dimensions(config: Config, folders: dict[str, Path]) -> None:
    portfolios = []
    for idx in range(1, config.portfolio_count + 1):
        portfolios.append(
            {
                "portfolio_id": f"PORT{idx:03d}",
                "portfolio_name": f"Portfolio {idx}",
                "portfolio_type": random.choice(["BALANCED", "GROWTH", "INCOME"]),
                "base_currency_code": random.choice(CURRENCIES),
                "risk_policy_name": random.choice(["STANDARD", "CONSERVATIVE", "AGGRESSIVE"]),
            }
        )

    instruments = []
    for idx in range(1, config.instrument_count + 1):
        asset_class = random.choice(ASSET_CLASSES)
        instruments.append(
            {
                "instrument_id": f"INS{idx:04d}",
                "instrument_name": f"Instrument {idx}",
                "ticker": f"TCK{idx:04d}",
                "isin": f"ISIN{idx:08d}",
                "asset_class_code": asset_class,
                "currency_code": random.choice(CURRENCIES),
                "issuer_name": f"Issuer {random.randint(1, 15)}",
                "coupon_rate": round(random.uniform(0.01, 0.08), 4) if asset_class == "BOND" else 0.0,
                "maturity_dt": date.today() + timedelta(days=random.randint(365, 3650)),
            }
        )

    counterparties = []
    for idx in range(1, config.counterparty_count + 1):
        counterparties.append(
            {
                "counterparty_id": f"CP{idx:03d}",
                "counterparty_name": f"Counterparty {idx}",
                "counterparty_type": random.choice(["BROKER", "CUSTODIAN", "BANK"]),
                "country_code": random.choice(["US", "GB", "IN", "DE"]),
                "credit_rating": random.choice(["AAA", "AA", "A", "BBB"]),
            }
        )

    currencies = [{"currency_code": code, "currency_name": code, "decimal_precision": 2} for code in CURRENCIES]
    asset_classes = [{"asset_class_code": code, "asset_class_name": code.title(), "risk_bucket": code} for code in ASSET_CLASSES]
    market_data_sources = [
        {
            "source_system_code": "MOCK_FEED",
            "source_system_name": "Mock Feed",
            "vendor_name": MARKET_DATA_SOURCE,
        }
    ]

    write_csv(folders["dimensions"] / "portfolios.csv", portfolios, list(portfolios[0].keys()))
    write_csv(folders["dimensions"] / "instruments.csv", instruments, list(instruments[0].keys()))
    write_csv(folders["dimensions"] / "counterparties.csv", counterparties, list(counterparties[0].keys()))
    write_csv(folders["dimensions"] / "currencies.csv", currencies, list(currencies[0].keys()))
    write_csv(folders["dimensions"] / "asset_classes.csv", asset_classes, list(asset_classes[0].keys()))
    write_csv(folders["dimensions"] / "market_data_sources.csv", market_data_sources, list(market_data_sources[0].keys()))


def generate_market_prices(config: Config, folders: dict[str, Path]) -> list[dict]:
    rows = []
    start_dt = date.today() - timedelta(days=365 * config.prices_years)
    end_dt = date.today()
    for instrument_idx in range(1, config.instrument_count + 1):
        price = random.uniform(40, 180)
        for price_dt in daterange(start_dt, end_dt):
            daily_shock = random.gauss(0.0004, 0.012)
            if STRESS_START <= price_dt <= STRESS_END:
                daily_shock += random.gauss(-0.015, 0.035)
            price = max(1.0, price * (1 + daily_shock))
            rows.append(
                {
                    "price_id": f"PRC{instrument_idx:04d}{price_dt.strftime('%Y%m%d')}",
                    "instrument_id": f"INS{instrument_idx:04d}",
                    "price_dt": price_dt.isoformat(),
                    "close_price": round(price, 4),
                    "return_pct": round(daily_shock, 6),
                    "volatility_proxy": round(abs(daily_shock) * 100, 4),
                    "currency_code": random.choice(CURRENCIES),
                    "source_system_code": "MOCK_FEED",
                }
            )
    write_csv(folders["market_prices_daily"] / "market_prices_daily.csv", rows, list(rows[0].keys()))
    return rows


def generate_transactions_positions_cashflows(config: Config, folders: dict[str, Path], market_prices: list[dict]) -> None:
    transactions = []
    positions = []
    cashflows = []
    start_dt = date.today() - timedelta(days=365 * config.transactions_years)
    end_dt = date.today()

    price_lookup: dict[tuple[str, str], float] = {}
    for row in market_prices:
        price_lookup[(row["instrument_id"], row["price_dt"])] = float(row["close_price"])

    for portfolio_idx in range(1, config.portfolio_count + 1):
        for txn_idx in range(1, 151):
            txn_dt = start_dt + timedelta(days=random.randint(0, (end_dt - start_dt).days))
            instrument_id = f"INS{random.randint(1, config.instrument_count):04d}"
            quantity = random.randint(10, 500)
            price = price_lookup.get((instrument_id, txn_dt.isoformat()), random.uniform(25, 150))
            gross = round(quantity * price, 2)
            fees = round(gross * random.uniform(0.0005, 0.0030), 2)
            transactions.append(
                {
                    "transaction_id": f"TXN{portfolio_idx:03d}{txn_idx:05d}",
                    "portfolio_id": f"PORT{portfolio_idx:03d}",
                    "instrument_id": instrument_id,
                    "counterparty_id": f"CP{random.randint(1, config.counterparty_count):03d}",
                    "trade_dt": txn_dt.isoformat(),
                    "settlement_dt": (txn_dt + timedelta(days=2)).isoformat(),
                    "transaction_type": random.choice(["BUY", "SELL"]),
                    "quantity": quantity,
                    "price": round(price, 4),
                    "gross_amount": gross,
                    "fees_amount": fees,
                    "net_amount": round(gross - fees, 2),
                    "currency_code": random.choice(CURRENCIES),
                }
            )

        for position_dt in daterange(start_dt, end_dt):
            for holding_idx in range(1, 11):
                instrument_id = f"INS{((portfolio_idx - 1) * 3 + holding_idx) % config.instrument_count + 1:04d}"
                quantity = random.randint(100, 5000)
                close_price = price_lookup.get((instrument_id, position_dt.isoformat()), random.uniform(25, 150))
                market_value = round(quantity * close_price, 2)
                positions.append(
                    {
                        "position_id": f"POS{portfolio_idx:03d}{holding_idx:03d}{position_dt.strftime('%Y%m%d')}",
                        "portfolio_id": f"PORT{portfolio_idx:03d}",
                        "instrument_id": instrument_id,
                        "position_dt": position_dt.isoformat(),
                        "quantity": quantity,
                        "end_of_day_price": round(close_price, 4),
                        "market_value": market_value,
                        "unrealized_pnl": round(market_value * random.uniform(-0.03, 0.04), 2),
                        "currency_code": random.choice(CURRENCIES),
                        "source_system_code": "MOCK_FEED",
                    }
                )

        initial_dt = start_dt
        cashflows.append(
            {
                "cashflow_id": f"CF{portfolio_idx:03d}00001",
                "portfolio_id": f"PORT{portfolio_idx:03d}",
                "instrument_id": "",
                "cashflow_dt": initial_dt.isoformat(),
                "cashflow_type": "INITIAL_INVESTMENT",
                "cashflow_amount": round(-1 * random.uniform(200000, 500000), 2),
                "currency_code": "USD",
            }
        )
        for flow_idx in range(2, 8):
            flow_dt = start_dt + timedelta(days=random.randint(45, 360))
            cashflows.append(
                {
                    "cashflow_id": f"CF{portfolio_idx:03d}{flow_idx:05d}",
                    "portfolio_id": f"PORT{portfolio_idx:03d}",
                    "instrument_id": "",
                    "cashflow_dt": flow_dt.isoformat(),
                    "cashflow_type": random.choice(["DIVIDEND", "COUPON", "CAPITAL_CALL", "DISTRIBUTION"]),
                    "cashflow_amount": round(random.uniform(-40000, 90000), 2),
                    "currency_code": "USD",
                }
            )

    write_csv(folders["transactions"] / "transactions.csv", transactions, list(transactions[0].keys()))
    write_csv(folders["positions_daily"] / "positions_daily.csv", positions, list(positions[0].keys()))
    write_csv(folders["cashflows"] / "cashflows.csv", cashflows, list(cashflows[0].keys()))


def main() -> None:
    random.seed(42)
    # Updated to use Unity Catalog volume path
    base_dir = Path("/Volumes/quant_core/landing/mock_data")
    config = Config(base_dir=base_dir)
    folders = ensure_dirs(config.base_dir)
    print("Generating dimension data...")
    generate_dimensions(config, folders)
    print("Generating market prices (this may take a few minutes)...")
    market_prices = generate_market_prices(config, folders)
    print("Generating transactions, positions, and cashflows...")
    generate_transactions_positions_cashflows(config, folders, market_prices)
    print(f"\n✓ Mock data generated successfully in Unity Catalog volume!")
    print(f"  Location: {config.base_dir}")
    print(f"  - Dimensions: 6 files")
    print(f"  - Market prices: {len(market_prices):,} rows")
    print(f"  - Transactions, positions, and cashflows generated")


if __name__ == "__main__":
    main()
