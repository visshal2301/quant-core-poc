"""Bronze table specs by dataset."""

from quant_core.ingestion.bronze.dimensions import DIMENSION_BRONZE_SPECS
from quant_core.ingestion.bronze.facts import FACT_BRONZE_SPECS

ALL_BRONZE_SPECS = FACT_BRONZE_SPECS + DIMENSION_BRONZE_SPECS

