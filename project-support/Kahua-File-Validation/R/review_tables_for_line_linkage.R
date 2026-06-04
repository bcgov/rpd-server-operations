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

# CBRE_TABLE_NAME <- "fin_dim_boma_account_hierarchy_vw"
# CBRE_TABLE_NAME <- "fin_dim_invoice_vw"
CBRE_TABLE_NAME <- "fin_fact_general_ledger_actuals_vw"
# general_ledger_skey, fact_general_ledger_actuals_skey, document_company_skey
# general_ledger_company_skey, vendor_skey, property_skey, legal_entity_skey
# business_unit_category_skey, contract_detail_skey, contract_work_item_skey,
# boma_code_skey, client_financial_calendar_skey
CBRE_TABLE_NAME <- "fin_dim_general_ledger_vw"
# general_ledger_skey,
CBRE_TABLE_NAME <- "fin_invoice_invoice_line_link_vw"
# invoice_invoice_line_skey, invoice_skey,invoice_line_skey
CBRE_TABLE_NAME <- "fin_jde_h1_5"
# property_skey
CBRE_TABLE_NAME <- "fin_invoice_invoice_line_link_vw"
# invoice_skey, invoice_invoice_line_skey, invoice_line_skey
CBRE_TABLE_NAME <- "fin_gl_actual_cost_distribition_detail_link_vw"
# fact_general_ledger_actuals_skey, cost_distribution_detail_skey
CBRE_TABLE_NAME <- "fin_purchase_order_purchase_order_line_link_vw"
# purchase_order_skey, purchase_order_line_skey, purchase_order_purchase_order_line_skey
CBRE_TABLE_NAME <- "fin_fact_cost_distribution_detail_vw"
# general_ledger_skey, cost_distribution_detail_skey, invoice_line_skey,
#  purchase_order_line_skey, property_skey, invoice_skey, vendor_skey
chunk_1 <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = "2020-05-01T00:00:00Z",
  end_time = "2026-06-05T00:00:00Z",
  max_pages = 10
)

data <- chunk_1 |>
  purrr::pluck("data")

colnames(data)
# fin_invoice_invoice_line_link_vw <- data
# fin_dim_general_ledger_vw <- data
# fin_fact_general_ledger_actuals <- data
# fin_gl_actual_cost_distribution_detail_link <- data
# fin_fact_cost_distribution_detail_vw <- data
