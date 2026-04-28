import pytest
from pyspark.sql import SparkSession
from quant_core.transforms.silver import add_surrogate_key, build_tracking_hash


class TestSilverTransformations:
    """Unit tests for silver layer transformation functions."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing."""
        return SparkSession.builder \
            .appName("quant-core-tests") \
            .master("local[2]") \
            .getOrCreate()
    
    def test_add_surrogate_key(self, spark):
        """Test surrogate key generation."""
        data = [
            ("USD", "US Dollar"),
            ("EUR", "Euro"),
            ("GBP", "British Pound")
        ]
        df = spark.createDataFrame(data, ["currency_code", "currency_name"])
        
        result_df = add_surrogate_key(df, "currency_code", "currency_sk")
        
        assert result_df.count() == 3
        assert "currency_sk" in result_df.columns
        
        # Check surrogate keys are sequential
        sks = [row.currency_sk for row in result_df.collect()]
        assert sorted(sks) == [1, 2, 3]
    
    def test_build_tracking_hash(self, spark):
        """Test tracking hash generation for SCD2."""
        data = [
            ("PORT001", "Portfolio A", "Active"),
            ("PORT002", "Portfolio B", "Active")
        ]
        df = spark.createDataFrame(data, ["portfolio_id", "portfolio_name", "status"])
        
        result_df = build_tracking_hash(df, ["portfolio_name", "status"])
        
        assert "attribute_hash" in result_df.columns
        assert result_df.filter("attribute_hash IS NOT NULL").count() == 2


class TestDataQuality:
    """Data quality validation tests."""
    
    def test_no_null_surrogate_keys(self, spark):
        """Ensure surrogate keys are never null."""
        # This would be an integration test against actual tables
        pass
    
    def test_no_duplicate_business_keys(self, spark):
        """Ensure no duplicate business keys in dimensions."""
        pass
