CREATE OR REPLACE VIEW quant_core.gold.vw_portfolio_risk_summary AS
SELECT
  v.portfolio_sk,
  v.as_of_date,
  v.business_as_of_ts,
  v.system_as_of_ts,
  v.confidence_level,
  v.var_amount,
  s.svar_amount
FROM quant_core.gold.risk_var_results v
LEFT JOIN quant_core.gold.risk_svar_results s
  ON v.portfolio_sk = s.portfolio_sk
 AND v.as_of_date = s.as_of_date
 AND v.confidence_level = s.confidence_level;

CREATE OR REPLACE VIEW quant_core.gold.vw_portfolio_performance_summary AS
SELECT
  c.portfolio_sk,
  c.beginning_dt,
  c.valuation_dt,
  c.cagr,
  i.irr_proxy AS irr,
  x.xirr_proxy AS xirr
FROM quant_core.gold.performance_cagr_results c
LEFT JOIN quant_core.gold.performance_irr_results i
  ON c.portfolio_sk = i.portfolio_sk
LEFT JOIN quant_core.gold.performance_xirr_results x
  ON c.portfolio_sk = x.portfolio_sk;

CREATE OR REPLACE VIEW quant_core.gold.vw_portfolio_risk_asof AS
SELECT *
FROM quant_core.gold.risk_var_results;

CREATE OR REPLACE VIEW quant_core.gold.vw_portfolio_performance_asof AS
SELECT *
FROM quant_core.gold.performance_cagr_results;
