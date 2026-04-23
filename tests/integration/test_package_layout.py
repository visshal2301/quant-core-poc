def test_repo_package_imports():
    import quant_core
    import quant_core.calculations.finance
    import quant_core.calculations.runtime
    import quant_core.ingestion.mock_data
    import quant_core.ingestion.runtime
    import quant_core.transforms.runtime

    assert quant_core is not None
