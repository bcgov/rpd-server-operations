# Load libraries
library(base64enc, quietly = TRUE, warn.conflicts = FALSE)
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(here, quietly = TRUE, warn.conflicts = FALSE)
library(httr2, quietly = TRUE, warn.conflicts = FALSE)
library(jsonlite, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate, quietly = TRUE, warn.conflicts = FALSE)
library(purrr, quietly = TRUE, warn.conflicts = FALSE)
library(tibble, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr, quietly = TRUE, warn.conflicts = FALSE)

library(odbc, quietly = TRUE, warn.conflicts = FALSE)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

# Load helper functions
source(here::here("utilities/R/utilities.R"))

# com_fact_property_hierarchy_extended_attribute_vw
# com_fact_property_hierarchy_vw
# fm_maintenance_plan_property_hierarchy_link_vw
# fm_fact_workorder_vw
# fa_fa_property_xrefs_manager_default
# fm_benchmark_dim_component_vw
# asset_number, property (5 digit number), and activity_skey
# fm_benchmark_fact_mr_task_vw
# asset_skey, property_skey (all the numbers that start with 2's), component_skey, task_skey,
# fm_benchmark_fact_ops_task_vw
# asset_skey, property_skey, task_skey
# fm_benchmark_property_asset_link_vw
# fm_benchmark_dim_asset_vw
# fm_dim_property_extended_attribute_vw
# es_dim_program_vw
# es_dim_service_account_vw
# es_energy_targets
# es_fact_accrual_vw
# es_dim_invoice_vw
# es_fact_allocated_invoice_calendarized_vw
# envizi utility data -electric, floor space, natural gas, sewer, water
# es_fact_financial_planning_vw
# es_fact_invoice_calendarized_vw
# es_fact_invoice_vw
# es_fact_service_account_vw
# es_report_actual_budget_accruals_vw
# es_fact_property_energy_usage_and_weather_trending_allocated_vw
# es_fact_property_energy_usage_and_weather_trending_vw
# es_fact_property_emission_trending_vw
# es_fact_property_rating_vw
# excel_master_building_list
# com_dim_property_vw
# envizi_property_grouping_vw
# es_report_combined_vw
# fin_dim_property_reporting_code_vw
# es_fact_allocation_by_reporing_group_vw
# es_fact_property_rating_vw_23apr_pobc
# es_report_combined_vw_23apr_pobc
# pjm_report_project_details_vw
# client_property_id, property_skey

# es_fact_property_rating_vw
#   No Data
# es_fact_service_account_vw
#   5319 rows of data
# es_fact_service_account_allocation_vw
#   No Data
# es_fact_property_energy_usage_and_weather_trending_vw
#   266 pages
# es_fact_property_energy_usage_and_weather_trending_allocated_vw
#   266 pages
# es_fact_property_emission_trending_vw
#   152 pages
# es_fact_program_action_vw
#   No Data
# es_fact_invoice_vw
#   273 pages...if searching for most recent date.
# es_fact_invoice_calendarized_vw
# es_fact_financial_planning_vw
# es_fact_allocation_by_reporing_group_vw
#   510 pages
# es_fact_allocated_invoice_calendarized_vw
#   3847 pages
# es_fact_accrual_vw
#   No Data
# es_fact_accrual_allocated_vw
#   No Data
# es_energy_targets
#   27 rows - goal targets for various services
# es_dim_service_account_vw
#   5318 rows
# es_dim_program_vw
#   No Data
# es_dim_program_action_plan_vw
#   No Data
# es_dim_invoice_vw
#   273 pages -- first 5k rows have mainly missing data columns??
# com_fact_property_reporting_group_vw

# es_dim_program_vw
# es_fact_accrual_vw
# es_fact_financial_planning_vw
# es_fact_property_rating_vw
# es_fact_property_rating_vw_23apr_pobc
# es_report_program_vw
# fact_cost_saving_vw
# fact_project_cash_flow_vw
# fin_dim_cost_center_hierarchy_vw
# fin_fact_financial_planning_vw
# fin_general_ledger_cost_center_hierarchy_link_vw
# fin_jde_master_vendors_vw
# fin_jde_vendor_master_map_vw
# fin_mapping_contract_vw
# fin_mapping_general_ledger_vw
# fin_mapping_gl_account_vw
# fin_mapping_vendor_vw
# fm_dim_invoice_vw
# fm_dim_purchase_order_vw
# pjm_fact_cost_saving_vw
# pjm_fact_project_cash_flow_vw
# archibus_pr

# Second list
# com_dim_client_vw
# com_dim_country_vw
# com_dim_site_vw
# com_edp_currency_translation_rate_vw
# com_fact_property_hierarchy_extended_attribute_vw
# com_fact_property_region_hierarchy_vw
# dim_client_vw
# es_dim_program_action_plan_vw
# es_dim_program_action_plan_vw_23apr_pobc
# es_dim_program_vw_23apr_pobc
# es_fact_accrual_allocated_vw
# es_fact_accrual_allocated_vw_23apr_pobc
# es_fact_accrual_vw_23apr_pobc
# es_fact_financial_planning_vw_23apr_pobc
# es_fact_program_action_vw
# es_fact_program_action_vw_23apr_pobc
# es_fact_service_account_allocation_vw
# fin_dim_invoice_line_vw
# fin_my_finance_reporting_alias_vw
# fin_my_finance_reporting_months_alias_vw
# fin_mybuy_po_detail_reporting_vw
# fin_ref_lookup_vw
# fin_si7_otsr_h1_3
# fin_vt_accountsummary_charts
# fin_vt_accountsummary_details
# fin_vt_client_initiatives
# fin_vt_clientsavings_target
# fin_vt_savingsoverview_charts
# fin_vt_savingsoverview_details
# fin_vt_serviceline_management_fee
# fin_vt_serviceline_savings_target
# fm_dim_business_unit_vw
# fm_dim_equipment_extended_attribute_vw
# fm_dim_invoice_line_vw
# fm_dim_purchase_order_line_vw
# fm_dim_workorder_extended_attribute_vw
# fm_fact_purchase_order_workorder_invoice_vw
# fm_fact_workorder_log_history_vw HTTP 504
# fm_fact_workorder_po_invoice_vw
# fm_si7_pm_schedule_report
# pobc_estimatedbudget_region
# vt_accountsummary_charts
# vt_accountsummary_details
# vt_client_initiatives
# vt_clientsavings_target
# vt_fx_reports
# vt_savingsoverview_charts
# vt_savingsoverview_details
# vt_serviceline_management_fee
# vt_serviceline_savings_target

CBRE_TABLE_NAME <- "pjm_fact_milestone_vw"

# Query API
chunk_1 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2010-06-01T00:00:00Z",
  end_time = "2026-07-21T00:00:00Z"
)

# only 273 pages...

raw_data <- chunk_1$data

chunk_2 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2023-01-01T00:00:00Z",
  end_time = "2023-06-01T00:00:00Z"
)

chunk_3 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2023-06-01T00:00:00Z",
  end_time = "2024-01-01T00:00:00Z"
)

chunk_4 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2024-01-01T00:00:00Z",
  end_time = "2024-06-01T00:00:00Z"
)

chunk_5 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2024-06-01T00:00:00Z",
  end_time = "2024-09-01T00:00:00Z"
)

chunk_6 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2024-09-01T00:00:00Z",
  end_time = "2025-01-01T00:00:00Z"
)

chunk_7 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2025-01-01T00:00:00Z",
  end_time = "2025-06-01T00:00:00Z"
)

chunk_8 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2025-06-01T00:00:00Z",
  end_time = "2026-01-01T00:00:00Z"
)

chunk_9 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2026-01-01T00:00:00Z",
  end_time = "2026-01-10T00:00:00Z"
)

raw_data <- rbind(
  # chunk_1$data,
  # chunk_2$data,
  # chunk_3$data
  # chunk_4$data,
  # chunk_5$data,
  # chunk_6$data,
  # chunk_7$data,
  # chunk_8$data,
  chunk_9$data
)
