ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"

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


# Subset
ProjectActuals <- DimProjectData |>
  filter(project_number == "K1012013") |>
  select(
    project_number,
    project_skey,
    project_name,
    project_created_date,
    project_comment,
    client_recoverable = csf_clientrecoverable,
    organization = csf_ministryparentorg,
    division = csf_branchchildorg,
    funding_type = csf_fundingtype,
    funding_source = csf_fundingsource,
    work_complete = csf_workcomplete,
    pjm_fee_percentage = csf_pjmfeepercentage
  ) |>
  left_join(FactProjectData, by = join_by(project_skey))


################################################################################
DimProject <- DimProjectData |>
  filter(project_number == "K1012013") |>
  select(
    project_number,
    project_skey,
    project_name,
    project_created_date,
    risk_status,
    schedule_health,
    budget_health,
    overall_project_health,
    csf_pjmfeepercentage,
    csf_totalforecastfinalcost,
    csf_ministryparentorg,
    csf_branchchildorg,
    csf_workcomplete,
    csf_fundingtype,
    csf_fundingsource,
    csf_estimatetype,
    csf_pmosource,
    csf_servicetype,
    standard_project_type,
    fusion_project_type
    # -c(
    #   client_project_number,
    #   client_project_status,
    #   source_unique_id,
    #   edp_update_ts,
    #   source_partition_id,
    # )
  )

FactProject <- DimProject |>
  left_join(FactProjectData, by = join_by(project_skey)) |>
  select(
    project_number,
    project_skey,
    project_name,
    project_phase,
    charter_date,
    project_created_date,
    target_finish_date,
    risk_status,
    schedule_health,
    budget_health,
    overall_project_health,
    estimated_budget,
    estimated_pjm_fees
  )

FactInvoice <- FactProject |>
  left_join(FactInvoiceData, by = join_by(project_skey)) |>
  select(
    project_number,
    project_skey,
    project_name,
    project_phase,
    charter_date,
    project_created_date,
    project_activity_skey,
    change_order_skey,
    invoice_skey,
    record_type,
    invoice_status,
    invoice_desc,
    work_completed_this_period,
    work_completed_to_date,
    payables_billed_total,
    payables_remitted_total,
    balance_to_finish,
    total_to_date,
    total_earned_to_date,
    scheduled_value
  )

DimInvoice <- FactInvoice |>
  left_join(DimInvoiceData, by = join_by(invoice_skey)) |>
  select(
    project_number,
    project_skey,
    project_name,
    project_phase,
    charter_date,
    project_created_date,
    project_activity_skey,
    change_order_skey,
    invoice_skey,
    record_type,
    invoice_status,
    invoice_desc,
    invoice_approval_status,
    period_from,
    period_to,
    payment_date,
    date_submitted,
    date_approved,
    work_completed_this_period,
    work_completed_to_date,
    payables_billed_total,
    payables_remitted_total,
    balance_to_finish,
    total_to_date,
    total_earned_to_date,
    scheduled_value
  ) |>
  arrange(payment_date)
