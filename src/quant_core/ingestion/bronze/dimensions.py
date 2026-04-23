"""Bronze ingestion specs for dimension snapshots."""

from quant_core.models import BronzeTableSpec


DIMENSION_BRONZE_SPECS = [
    BronzeTableSpec(dataset="portfolios", table_name="portfolios_raw", expected_grain="dimension_snapshot", is_dimension=True),
    BronzeTableSpec(dataset="instruments", table_name="instruments_raw", expected_grain="dimension_snapshot", is_dimension=True),
    BronzeTableSpec(dataset="counterparties", table_name="counterparties_raw", expected_grain="dimension_snapshot", is_dimension=True),
    BronzeTableSpec(dataset="currencies", table_name="currencies_raw", expected_grain="dimension_snapshot", is_dimension=True),
    BronzeTableSpec(dataset="asset_classes", table_name="asset_classes_raw", expected_grain="dimension_snapshot", is_dimension=True),
    BronzeTableSpec(dataset="market_data_sources", table_name="market_data_sources_raw", expected_grain="dimension_snapshot", is_dimension=True),
]

