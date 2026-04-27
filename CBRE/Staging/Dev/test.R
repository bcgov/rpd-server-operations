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
pjm_fact_project_comment_vw
pjm_fact_project_risk_vw
pjm_fact_project_service_vw
pjm_fact_project_vw
pjm_kahua_h2_3_fullsnapshot
pjm_report_milestone_vw
pjm_report_project_details_vw
pjm_report_project_role_vw


dim_project_activity_vw <- extract_cbre_data(
  "dim_project_activity_vw",
  max_pages = 2
)
dim_project_role_vw <- extract_cbre_data("dim_project_role_vw", max_pages = 2)
dim_project_vw <- extract_cbre_data("dim_project_vw", max_pages = 2)
es_fact_allocation_by_reporing_group_vw <- extract_cbre_data(
  "es_fact_allocation_by_reporing_group_vw",
  max_pages = 2
)
fact_budget_vw <- extract_cbre_data("fact_budget_vw", max_pages = 2)
fact_contract_vw <- extract_cbre_data("fact_contract_vw", max_pages = 2)
fact_invoice_vw <- extract_cbre_data("fact_invoice_vw", max_pages = 2)
fact_milestone_vw <- extract_cbre_data("fact_milestone_vw", max_pages = 2)
fact_project_activity_vw <- extract_cbre_data(
  "fact_project_activity_vw",
  max_pages = 2
)
fact_project_risk_vw <- extract_cbre_data("fact_project_risk_vw", max_pages = 2)
fact_project_role_vw <- extract_cbre_data("fact_project_role_vw", max_pages = 2)
fact_project_vw <- extract_cbre_data("fact_project_vw", max_pages = 2)
fin_dim_general_ledger_vw <- extract_cbre_data(
  "fin_dim_general_ledger_vw",
  max_pages = 2
)
fin_fact_general_ledger_actuals_vw <- extract_cbre_data(
  "fin_fact_general_ledger_actuals_vw",
  max_pages = 2
)
fm_report_worker_role_vw <- extract_cbre_data(
  "fm_report_worker_role_vw",
  max_pages = 2
)


fetch_edp_csv(c)
