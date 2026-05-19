ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"

options(scipen = 999)

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
# K1012013
#
# 2,830.15
# 6,814.80
# 7176.85
# 228940
#
#
# 682200.68
#
# STOB2000

# Load helper functions
source(here::here("./utilities/R/cbre_api_function.R"))
source(here::here("./utilities/R/event_logger.R"))
source(here::here("./utilities/R/sql_helper_functions.R"))

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query SQL Datasets ####
query <- dbSendQuery(con, "SELECT * FROM CbreStaging.dim_project")
DimProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.dim_invoice")
DimInvoiceData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.fact_project")
FactProjectData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.fact_invoice")
FactInvoiceData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.dim_project_activity")
DimProjectActivityData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.fact_project_activity")
FactProjectActivityData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_dim_change_order")
DimChangeOrderData <- dbFetch(query, n = -1)
dbClearResult(query)

query <- dbSendQuery(con, "SELECT * FROM CbreStaging.pjm_fact_change_order")
FactChangeOrderData <- dbFetch(query, n = -1)
dbClearResult(query)

# FIX fact_project estimated_budget, estimated_pjm_fees as.double

# Subset
ProjectActuals <- DimProjectData |>
  filter(project_number == "K1012013") |>
  select(
    project_number,
    project_skey,
    project_name,
    project_created_date,
    project_comment,
    project_budget,
    client_recoverable = csf_clientrecoverable,
    organization = csf_ministryparentorg,
    division = csf_branchchildorg,
    funding_type = csf_fundingtype,
    funding_source = csf_fundingsource,
    work_complete = csf_workcomplete,
    pjm_fee_percentage = csf_pjmfeepercentage
  ) |>
  left_join(FactProjectData, by = join_by(project_skey)) |>
  select(
    project_number,
    project_skey,
    project_name,
    project_created_date,
    target_finish_date,
    charter_date,
    project_comment,
    project_budget,
    client_recoverable,
    organization,
    division,
    funding_type,
    funding_source,
    work_complete,
    estimated_budget,
    estimated_pjm_fees,
    pjm_fee_percentage
  ) |>
  mutate(
    across(
      c(
        estimated_budget,
        estimated_pjm_fees
      ),
      as.double
    )
  ) |>
  left_join(FactInvoiceData, by = join_by(project_skey)) |>
  select(
    -c(
      RefreshDate,
      source_unique_id,
      source_created_ts,
      source_modified_ts,
      source_partition_id,
      source_system_code,
      edp_update_ts
    )
  ) |>
  left_join(DimInvoiceData, by = join_by(invoice_skey)) |>
  select(
    -c(
      RefreshDate,
      source_unique_id,
      source_created_ts,
      source_modified_ts,
      source_partition_id,
      source_system_code,
      edp_update_ts
    )
  ) |>
  select(
    project_number,
    project_skey,
    project_created_date,
    target_finish_date,
    charter_date,
    invoice_date_submitted = date_submitted,
    invoice_date_approved = date_approved,
    project_comment,
    project_budget,
    estimated_budget,
    estimated_pjm_fees,
    invoice_record_type = record_type,
    project_activity_skey,
    change_order_skey,
    invoice_status,
    invoice_approval_status,
    invoice_id,
    invoice_item_id,
    invoice_desc,
    contract_original_amount,
    contract_approved_change_amount,
    current_payment_due,
    invoice_payment_date = payment_date,
    period_from,
    period_to,
    work_completed_this_period,
    work_completed_to_date,
    work_retainage,
    work_retainage_percent,
    payables_billed_total,
    payables_withheld_total,
    payables_remitted_total,
    balance_to_finish,
    balance_to_finish_with_retainage,
    previous_total_earned,
    previous_work_completed,
    total_to_date,
    total_earned_to_date,
    total_retainage,
    scheduled_value
  )

Invoices <- ProjectActuals |>
  filter(invoice_record_type == "invoice")

InvoiceItems <- ProjectActuals |>
  filter(invoice_record_type == "invoice_item")

distinctInvoice <- Invoices |>
  select(invoice_id) |>
  filter(!is.na(invoice_id)) |>
  distinct()

distinctInvoiceItems <- InvoiceItems |>
  select(invoice_id) |>
  filter(!is.na(invoice_id)) |>
  distinct()

# SumInvoiceItems <- InvoiceItems |>
#   group_by(project_number, invoice_desc) |>
#   summarise()

# openxlsx2::write_xlsx(
#   InvoiceItems,
#   here::here("output/ProjectForecasting/InvoiceItems.xlsx")
# )
ChangeOrder <- FactChangeOrderData |>
  select(
    project_skey,
    project_activity_skey,
    change_order_skey,
    fco_status = status,
    cost_type,
    change_order_reason_summary,
    change_order_item_skey,
    change_order_item_number,
    change_order_item_id,
    change_order_item_desc,
    change_order_item_amount
  ) |>
  left_join(
    DimChangeOrderData,
    by = join_by(project_skey, change_order_skey)
  ) |>
  select(
    project_skey,
    project_activity_skey,
    change_order_skey,
    contract_skey,
    fco_status,
    dco_status = status,
    change_order_form_template,
    cost_type,
    change_order_reason_summary,
    change_order_item_skey,
    change_order_id,
    change_order_number,
    change_order_item_number,
    change_order_item_id,
    change_order_item_desc,
    change_order_desc,
    change_order_item_amount,
    change_order_amount
  )


FullInvoiceItems <- InvoiceItems |>
  left_join(
    ChangeOrder,
    by = join_by(project_skey, project_activity_skey, change_order_skey)
  ) |>
  select(
    project_number,
    # project_comment,
    project_created_date,
    charter_date,
    target_finish_date,
    invoice_id,
    invoice_date_submitted,
    invoice_date_approved,
    invoice_record_type,
    invoice_status,
    invoice_desc,
    # project_budget,
    estimated_budget,
    contract_original_amount,
    contract_approved_change_amount,
    change_order_status = fco_status,
    change_order_desc,
    change_order_item_desc,
    change_order_reason_summary,
    change_order_item_amount,
    change_order_amount,
    current_payment_due,
    invoice_payment_date,
    period_from,
    period_to,
    work_completed_this_period,
    work_completed_to_date,
    previous_work_completed,
    previous_total_earned,
    payables_billed_total,
    payables_withheld_total,
    payables_remitted_total,
    work_retainage,
    work_retainage_percent,
    balance_to_finish,
    balance_to_finish_with_retainage,
    total_to_date,
    total_earned_to_date,
    total_retainage,
    scheduled_value
  )

DatedInvoiceItems <- FullInvoiceItems |>
  filter(!is.na(invoice_date_submitted)) |>
  arrange(desc(invoice_date_submitted))

UnDatedInvoiceItems <- FullInvoiceItems |>
  filter(is.na(invoice_date_submitted))

openxlsx2::write_xlsx(
  FullInvoiceItems,
  here::here("output/ProjectForecasting/InvoiceItems-K1012013.xlsx")
)
