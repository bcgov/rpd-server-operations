# For server logging
# Begin timer
task_start <- Sys.time()

# Load helper functions
source(here::here("utilities/R/utilities.R"))

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

# Setup necessary variables
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "fact_contract"
CBRE_TABLE_NAME <- "fact_contract_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "fact_contract"

# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

raw_data <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = etl_window$start_time,
  end_time = etl_window$end_time
)

clean_data <- raw_data |>
  purrr::pluck("data") |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
  mutate(
    across(
      c(
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  select(
    RefreshDate,
    edp_update_ts,
    project_skey,
    contract_skey,
    contract_id,
    record_type,
    cost_type,
    status_code,
    contract_item_skey,
    contract_item_id,
    line_number,
    project_activity_skey,
    vendor_project_manager_skey,
    vendor_company_skey,
    vendor_contact_skey,
    client_contact_skey,
    client_company_skey,
    original_contract_amount,
    contract_gross_total_amount,
    contract_item_desc,
    cost_current_total,
    cost_estimated,
    cost_estimated_quantity,
    cost_estimated_total,
    cost_original_total_value,
    cost_pending_commitments,
    cost_pending_commitments_uom,
    cost_pending_commitments_total,
    cost_pending_commitments_quantity,
    contract_items_total_amount,
    approved_changes_amount,
    compensation,
    executed_changes_amount,
    executed_changes_contract_amount,
    cost_items_total_total_value,
    cost_estimated_uom,
    cost_pending_commitments,
    cost_pending_commitments_quantity,
    scope_of_work,
    contract_item_notes,
    kahua_po_number,
    source_unique_id,
    source_partition_id,
    source_system_code,
    source_created_ts
  )

dbWriteTable(con, TARGET_TABLE, clean_data)
