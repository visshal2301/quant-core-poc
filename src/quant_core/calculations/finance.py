"""Pure finance calculation helpers."""

from datetime import date


def irr(values, guess=0.10, max_iterations=100, tolerance=1e-7):
    try:
        if not values or len(values) < 2:
            return None
        positive_count = sum(1 for value in values if value > 0)
        negative_count = sum(1 for value in values if value < 0)
        if positive_count == 0 or negative_count == 0:
            return None

        rate = guess
        min_rate = -0.99
        max_rate = 100.0
        for _ in range(max_iterations):
            if rate < min_rate or rate > max_rate:
                return None
            npv = 0.0
            derivative = 0.0
            for period_index, amount in enumerate(values):
                try:
                    if period_index > 0 and abs(1 + rate) > 1e10:
                        return None
                    power_term = (1 + rate) ** period_index
                    npv += amount / power_term
                    if period_index > 0:
                        derivative -= period_index * amount / ((1 + rate) ** (period_index + 1))
                except (OverflowError, ZeroDivisionError):
                    return None
            if abs(npv) < tolerance:
                return float(rate)
            if derivative == 0 or abs(derivative) < 1e-10:
                break
            rate_delta = npv / derivative
            max_step = 0.5
            if abs(rate_delta) > max_step:
                rate_delta = max_step if rate_delta > 0 else -max_step
            rate -= rate_delta
        return None
    except Exception:
        return None


def xirr(cashflow_series, guess=0.10, max_iterations=100, tolerance=1e-7):
    try:
        parsed = sorted(
            [(item["cashflow_dt"], float(item["cashflow_amount"])) for item in cashflow_series if item["cashflow_dt"] is not None],
            key=lambda item: item[0],
        )
        if len(parsed) < 2:
            return None
        positive_count = sum(1 for _, amount in parsed if amount > 0)
        negative_count = sum(1 for _, amount in parsed if amount < 0)
        if positive_count == 0 or negative_count == 0:
            return None

        start_dt = parsed[0][0]
        if isinstance(start_dt, str):
            start_dt = date.fromisoformat(start_dt)

        def xnpv(rate):
            total = 0.0
            for cashflow_dt, amount in parsed:
                dt_value = cashflow_dt if not isinstance(cashflow_dt, str) else date.fromisoformat(cashflow_dt)
                year_fraction = (dt_value - start_dt).days / 365.25
                try:
                    if abs(1 + rate) > 1e10 or year_fraction * abs(rate) > 50:
                        raise OverflowError
                    total += amount / ((1 + rate) ** year_fraction)
                except (OverflowError, ZeroDivisionError):
                    return float("inf") if rate > 0 else float("-inf")
            return total

        rate = guess
        min_rate = -0.99
        max_rate = 100.0
        for _ in range(max_iterations):
            if rate < min_rate or rate > max_rate:
                return None
            value = xnpv(rate)
            if not (-1e15 < value < 1e15):
                return None
            if abs(value) < tolerance:
                return float(rate)
            derivative = (xnpv(rate + tolerance) - value) / tolerance
            if derivative == 0 or abs(derivative) < 1e-10 or not (-1e15 < derivative < 1e15):
                break
            rate_delta = value / derivative
            max_step = 0.5
            if abs(rate_delta) > max_step:
                rate_delta = max_step if rate_delta > 0 else -max_step
            rate -= rate_delta
        return None
    except Exception:
        return None

