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
source(here::here("rpd-server-operations/utilities/R/cbre_api_function.R"))
source(here::here("rpd-server-operations/utilities/R/event_logger.R"))

# Entities of interest
# kahua_cashflow
kahua_cashflow <- extract_cbre_data("kahua_cashflow", max_pages = 5) # FAILED
# kahua_master_hierarchy
kahua_master_hierarchy <- extract_cbre_data(
  "kahua_master_hierarchy",
  max_pages = 5
) # 13 pages # STAGED
# kahua_milestones
kahua_milestones <- extract_cbre_data("kahua_milestones", max_pages = 5) # FAILED
# kahua_project_role
kahua_project_role <- extract_cbre_data("kahua_project_role", max_pages = 5) # FAILED
# pjm_kahua_project_projectsplitting
pjm_kahua_project_projectsplitting <- extract_cbre_data(
  "pjm_kahua_project_projectsplitting",
  max_pages = 5
) # FAILED
# pjm_kahua_h1_3_fullsnapshot
pjm_kahua_h1_3_fullsnapshot <- extract_cbre_data(
  "pjm_kahua_h1_3_fullsnapshot",
  max_pages = 5
) # 6 pages
# pjm_kahua_project_projectsplittingrecords
pjm_kahua_project_projectsplittingrecords <- extract_cbre_data(
  "pjm_kahua_project_projectsplittingrecords",
  max_pages = 5
) # FAILED

# pjm_dim_budget_vw
pjm_dim_budget_vw <- extract_cbre_data("pjm_dim_budget_vw", max_pages = 5) # 15 # STAGED
# pjm_dim_change_order_vw
pjm_dim_change_order_vw <- extract_cbre_data(
  "pjm_dim_change_order_vw",
  max_pages = 5
) # 16 # STAGED
# pjm_dim_company_vw
pjm_dim_company_vw <- extract_cbre_data("pjm_dim_company_vw", max_pages = 5) # 1
# pjm_dim_contact_vw
pjm_dim_contact_vw <- extract_cbre_data("pjm_dim_contact_vw", max_pages = 5) # 2
# pjm_dim_contract_vw
pjm_dim_contract_vw <- extract_cbre_data("pjm_dim_contract_vw", max_pages = 5) # 20
# pjm_dim_invoice_vw
pjm_dim_invoice_vw <- extract_cbre_data("pjm_dim_invoice_vw", max_pages = 5) # 54
# pjm_dim_project_activity_vw
pjm_dim_project_activity_vw <- extract_cbre_data(
  "pjm_dim_project_activity_vw",
  max_pages = 5
) # 127
# pjm_dim_project_communication_vw
pjm_dim_project_communication_vw <- extract_cbre_data(
  "pjm_dim_project_communication_vw",
  max_pages = 5
) # FAILED
# pjm_dim_project_role_vw
pjm_dim_project_role_vw <- extract_cbre_data(
  "pjm_dim_project_role_vw",
  max_pages = 5
) # 1
# pjm_dim_project_vw
pjm_dim_project_vw <- extract_cbre_data("pjm_dim_project_vw", max_pages = 5) # 8
# pjm_excel_calendar_d09x
pjm_excel_calendar_d09x <- extract_cbre_data(
  "pjm_excel_calendar_d09x",
  max_pages = 5
) # 1
# pjm_excel_fy2526_budgets
pjm_excel_fy2526_budgets <- extract_cbre_data(
  "pjm_excel_fy2526_budgets",
  max_pages = 5
) # 1
# pjm_excel_fy2526_vow_expense
pjm_excel_fy2526_vow_expense <- extract_cbre_data(
  "pjm_excel_fy2526_vow_expense",
  max_pages = 5
) # 1
# pjm_excel_fy2526_vow_rc_capital
pjm_excel_fy2526_vow_rc_capital <- extract_cbre_data(
  "pjm_excel_fy2526_vow_rc_capital",
  max_pages = 5
) # 1
# pjm_excel_pobc_program_cashflow_report_byfy_rpdpartition_v1
pjm_excel_pobc_program_cashflow_report_byfy_rpdpartition_v1 <- extract_cbre_data(
  "pjm_excel_pobc_program_cashflow_report_byfy_rpdpartition_v1",
  max_pages = 5
) # 76
# pjm_fact_budget_vw
pjm_fact_budget_vw <- extract_cbre_data("pjm_fact_budget_vw", max_pages = 5) # 582
# pjm_fact_change_order_vw
pjm_fact_change_order_vw <- extract_cbre_data(
  "pjm_fact_change_order_vw",
  max_pages = 5
) # 17
# pjm_fact_contract_vw
pjm_fact_contract_vw <- extract_cbre_data("pjm_fact_contract_vw", max_pages = 5) # 46
# pjm_fact_cost_saving_vw
pjm_fact_cost_saving_vw <- extract_cbre_data(
  "pjm_fact_cost_saving_vw",
  max_pages = 5
) # FAILED
# pjm_fact_invoice_vw
pjm_fact_invoice_vw <- extract_cbre_data("pjm_fact_invoice_vw", max_pages = 5) # 227
# pjm_fact_milestone_vw
pjm_fact_milestone_vw <- extract_cbre_data(
  "pjm_fact_milestone_vw",
  max_pages = 5
) # 82
# pjm_fact_project_activity_vw
pjm_fact_project_activity_vw <- extract_cbre_data(
  "pjm_fact_project_activity_vw",
  max_pages = 5
) # 127
# pjm_fact_project_cash_flow_vw
pjm_fact_project_cash_flow_vw <- extract_cbre_data(
  "pjm_fact_project_cash_flow_vw",
  max_pages = 5
) # FAILED
# pjm_fact_project_comment_vw
pjm_fact_project_comment_vw <- extract_cbre_data(
  "pjm_fact_project_comment_vw",
  max_pages = 5
) # 1
# pjm_fact_project_risk_vw
pjm_fact_project_risk_vw <- extract_cbre_data(
  "pjm_fact_project_risk_vw",
  max_pages = 5
) # 1
# pjm_fact_project_role_vw
pjm_fact_project_role_vw <- extract_cbre_data(
  "pjm_fact_project_role_vw",
  max_pages = 5
) # 13
# pjm_fact_project_service_vw
pjm_fact_project_service_vw <- extract_cbre_data(
  "pjm_fact_project_service_vw",
  max_pages = 5
) # 1
# pjm_fact_project_vw
pjm_fact_project_vw <- extract_cbre_data("pjm_fact_project_vw", max_pages = 5) # 8
# pjm_fact_value_creation_vw
pjm_fact_value_creation_vw <- extract_cbre_data(
  "pjm_fact_value_creation_vw",
  max_pages = 5
) # 1
# pjm_project_communication_project_link_vw
pjm_project_communication_project_link_vw <- extract_cbre_data(
  "pjm_project_communication_project_link_vw",
  max_pages = 5
) # 1
# pjm_report_milestone_vw
pjm_report_milestone_vw <- extract_cbre_data(
  "pjm_report_milestone_vw",
  max_pages = 5
) # 8
# pjm_report_project_details_vw
pjm_report_project_details_vw <- extract_cbre_data(
  "pjm_report_project_details_vw",
  max_pages = 5
) # 8
# pjm_report_project_role_vw
pjm_report_project_role_vw <- extract_cbre_data(
  "pjm_report_project_role_vw",
  max_pages = 5
) # 8
