# THIS ONE IS WAY TO LARGE TO QUERY NORMALLY
# MAYBE SETUP AN INCREMENTAL REFRESH

ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "fact_budget"
CBRE_TABLE_NAME <- "fact_budget_vw"

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

target_table <- Id(schema = SCHEMA_NAME, table = TABLE_NAME)
temp_table <- paste0("#", TABLE_NAME, "Temp")

# edp_tables <- list(
#   "dim_project_activity_vw",
#   "dim_project_role_vw",
#   "fact_budget_vw",
#   "fact_invoice_vw",
#   "fact_project_activity_vw"
# )
# ---- submit all exports ----
# jobs <- lapply(edp_tables, function(tbl) {
#   submit_edp_export(
#     edp_table = tbl
#   )
# })
# results <- lapply(jobs, function(job) {
#   retrieve_edp_export(job$file_id)
# })
#
# file_id <- jobs[[3]]$file_id
#
# jobs[[3]]$edp_table
# output <- retrieve_edp_export(file_id = file_id)
#
# retrieve_edp_export <- function(
#     file_id,
#     base_url = "https://api.cbre.com:443/",
#     file_endpoint = "/t/digitaltech_us_edp/vantageanalytics/prod/v1/file/",
#     username = "kWNLCjcu05JmqrlSsmDtMoSreuUa",
#     keyring_service = "CBRE_API",
#     poll_interval = 60,
#     timeout = 1200
# ) {
#   credentials <- keyring::key_get(
#     service = keyring_service,
#     username = username
#   )
#
#   start_time <- Sys.time()
#
#   # ---- wait until first file is available ----
#   repeat {
#     bearer_token <- get_bearer_token(base_url, username, credentials)
#
#     probe <- try(
#       request(base_url) |>
#         httr2::req_url_path_append(file_endpoint) |>
#         httr2::req_url_path_append(file_id) |>
#         httr2::req_headers("file-num" = "1") |>
#         httr2::req_auth_bearer_token(bearer_token) |>
#         httr2::req_perform(),
#       silent = TRUE
#     )
#
#     if (!inherits(probe, "try-error") && resp_status(probe) == 200) {
#       print("successful response")
#       break
#     }
#
#     if (difftime(Sys.time(), start_time, units = "secs") > timeout) {
#       stop("Timed out waiting for EDP export to be ready")
#     }
#     print("Going for another loop")
#     Sys.sleep(poll_interval)
#   }
#
#   # ---- retrieve all files ----
#   all_data <- NULL
#   fileNumber <- "1"
#   lastFile <- "false"
#
#   while (lastFile != "true") {
#     bearer_token <- get_bearer_token(base_url, username, credentials)
#
#     resp <- request(base_url) |>
#       httr2::req_url_path_append(file_endpoint) |>
#       httr2::req_url_path_append(file_id) |>
#       httr2::req_headers("file-num" = fileNumber) |>
#       httr2::req_auth_bearer_token(bearer_token) |>
#       httr2::req_perform()
#
#     chunk <- resp |>
#       resp_body_string() |>
#       readr::read_csv(col_types = readr::cols(.default = "c"))
#
#     all_data <- dplyr::bind_rows(all_data, chunk)
#
#     fileNumber <- resp$headers$`next-file-num`
#     lastFile <- resp$headers$`last-file`
#   }
#
#   all_data
# }

raw_data <- all_data

cleaned_data <- raw_data |>
  select_if(~ !all(is.na(.))) |>
  select_if(~ !all(. == 0)) |>
  select_if(~ !all(. == '-1')) |>
  select_if(~ !all(. == "N/A")) |>
  select_if(~ !all(. == "-")) |>
  mutate(
    across(
      c(
        edp_update_ts,
        source_modified_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%d %H:%M:%S %z")
    )
  ) |>
  mutate(
    across(
      c(
        approved_total_budget_value,
        approved_budget_capital_total,
        estimated_budget_capital,
        approved_budget_expense,
        estimated_budget_total_value,
        budget_capital_value,
        budget_item_total_value,
        budget_expense_amount,
        estimated_budget_expense,
        approved_budget_expense_total
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        project_skey,
        project_activity_skey,
        budget_skey,
        approved_by_skey,
        authorized_by_skey,
        submitted_by_skey,
        budget_item_skey
      ),
      as.integer
    )
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.Date())) |>
  select(
    RefreshDate,
    project_skey,
    project_activity_skey,
    budget_skey,
    approved_by_skey,
    authorized_by_skey,
    submitted_by_skey,
    record_type,
    costtype,
    budget_desc,
    budget_item_skey,
    budget_item_id,
    approved_total_budget_value,
    approved_budget_capital_total,
    estimated_budget_capital,
    approved_budget_expense,
    estimated_budget_total_value,
    budget_capital_value,
    budget_status,
    budget_item_total_value,
    estimated_budget_uom,
    estimated_budget_quantity,
    budget_expense_amount,
    budget_notes,
    estimated_budget_expense,
    line_number,
    approved_budget_expense_total,
    estimated_budget_per_uom,
    edp_update_ts,
    source_unique_id,
    source_system_code,
    source_partition_id,
    source_modified_ts
  )

# dbRemoveTable(con, target_table)
if (!dbExistsTable(con, target_table)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
        RefreshDate                     DATETIME2(3)  NOT NULL,
        project_skey                    INT           NOT NULL,
        project_activity_skey           INT           NULL,
        budget_skey                     INT           NOT NULL,
        approved_by_skey                INT           NULL,
        authorized_by_skey              INT           NULL,
        submitted_by_skey               INT           NULL,
        record_type                     NVARCHAR(20)  NULL,
        costtype                        NVARCHAR(20)  NULL,
        budget_desc                     NVARCHAR(250) NULL,
        budget_item_skey                INT           NOT NULL,
        budget_item_id                  NVARCHAR(20)  NULL,
        approved_total_budget_value     DECIMAL(18,2) NULL,
        approved_budget_capital_total   DECIMAL(18,2) NULL,
        estimated_budget_capital        DECIMAL(18,2) NULL,
        approved_budget_expense         DECIMAL(18,2) NULL,
        estimated_budget_total_value    DECIMAL(18,2) NULL,
        budget_capital_value            DECIMAL(18,2) NULL,
        budget_status                   NVARCHAR(20)  NULL,
        budget_item_total_value         DECIMAL(18,2) NULL,
        estimated_budget_uom            NVARCHAR(20)  NULL,
        estimated_budget_quantity       NVARCHAR(20)  NULL,
        budget_expense_amount           DECIMAL(18,2) NULL,
        budget_notes                    NVARCHAR(900) NULL,
        estimated_budget_expense        DECIMAL(18,2) NULL,
        line_number                     NVARCHAR(20)  NULL,
        approved_budget_expense_total   DECIMAL(18,2) NULL,
        estimated_budget_per_uom        NVARCHAR(30)  NULL,
        edp_update_ts                   DATETIME2(3)  NULL,
        source_unique_id                NVARCHAR(20)  NULL,
        source_system_code              NVARCHAR(20)  NULL,
        source_partition_id             NVARCHAR(20)  NULL,
        source_modified_ts              DATETIME2(3)  NULL
        );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####
# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and rollback of transaction on failure
tryCatch(
  {
    if (dbExistsTable(con, temp_table)) {
      dbRemoveTable(con, temp_table)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        temp_table,
        " (
          RefreshDate                     DATETIME2(3)  NOT NULL,
          project_skey                    INT           NOT NULL,
          project_activity_skey           INT           NULL,
          budget_skey                     INT           NOT NULL,
          approved_by_skey                INT           NULL,
          authorized_by_skey              INT           NULL,
          submitted_by_skey               INT           NULL,
          record_type                     NVARCHAR(20)  NULL,
          costtype                        NVARCHAR(20)  NULL,
          budget_desc                     NVARCHAR(250) NULL,
          budget_item_skey                INT           NULL,
          budget_item_id                  NVARCHAR(20)  NULL,
          approved_total_budget_value     DECIMAL(18,2) NULL,
          approved_budget_capital_total   DECIMAL(18,2) NULL,
          estimated_budget_capital        DECIMAL(18,2) NULL,
          approved_budget_expense         DECIMAL(18,2) NULL,
          estimated_budget_total_value    DECIMAL(18,2) NULL,
          budget_capital_value            DECIMAL(18,2) NULL,
          budget_status                   NVARCHAR(20)  NULL,
          budget_item_total_value         DECIMAL(18,2) NULL,
          estimated_budget_uom            NVARCHAR(20)  NULL,
          estimated_budget_quantity       NVARCHAR(20)  NULL,
          budget_expense_amount           DECIMAL(18,2) NULL,
          budget_notes                    NVARCHAR(900) NULL,
          estimated_budget_expense        DECIMAL(18,2) NULL,
          line_number                     NVARCHAR(20)  NULL,
          approved_budget_expense_total   DECIMAL(18,2) NULL,
          estimated_budget_per_uom        NVARCHAR(30)  NULL,
          edp_update_ts                   DATETIME2(3)  NULL,
          source_unique_id                NVARCHAR(20)  NULL,
          source_system_code              NVARCHAR(20)  NULL,
          source_partition_id             NVARCHAR(20)  NULL,
          source_modified_ts              DATETIME2(3)  NULL
  );
  "
      )
    )

    # Write the current tibble into the temp table
    dbWriteTable(
      con,
      name = temp_table,
      value = cleaned_data,
      append = TRUE,
      overwrite = FALSE
    )

    # Update existing rows in the target table
    n_updated <- dbExecute(
      con,
      paste0(
        "
    UPDATE tgt
    SET
      tgt.RefreshDate                   = src.RefreshDate,
      tgt.project_skey                  = src.project_skey,
      tgt.project_activity_skey         = src.project_activity_skey,
      tgt.budget_skey                   = src.budget_skey,
      tgt.approved_by_skey              = src.approved_by_skey,
      tgt.authorized_by_skey            = src.authorized_by_skey,
      tgt.submitted_by_skey             = src.submitted_by_skey,
      tgt.record_type                   = src.record_type,
      tgt.costtype                      = src.costtype,
      tgt.budget_desc                   = src.budget_desc,
      tgt.budget_item_skey              = src.budget_item_skey,
      tgt.budget_item_id                = src.budget_item_id,
      tgt.approved_total_budget_value   = src.approved_total_budget_value,
      tgt.approved_budget_capital_total = src.approved_budget_capital_total,
      tgt.estimated_budget_capital      = src.estimated_budget_capital,
      tgt.approved_budget_expense       = src.approved_budget_expense,
      tgt.estimated_budget_total_value  = src.estimated_budget_total_value,
      tgt.budget_capital_value          = src.budget_capital_value,
      tgt.budget_status                 = src.budget_status,
      tgt.budget_item_total_value       = src.budget_item_total_value,
      tgt.estimated_budget_uom          = src.estimated_budget_uom,
      tgt.estimated_budget_quantity     = src.estimated_budget_quantity,
      tgt.budget_expense_amount         = src.budget_expense_amount,
      tgt.budget_notes                  = src.budget_notes,
      tgt.estimated_budget_expense      = src.estimated_budget_expense,
      tgt.line_number                   = src.line_number,
      tgt.approved_budget_expense_total = src.approved_budget_expense_total,
      tgt.estimated_budget_per_uom      = src.estimated_budget_per_uom,
      tgt.edp_update_ts                 = src.edp_update_ts,
      tgt.source_unique_id              = src.source_unique_id,
      tgt.source_system_code            = src.source_system_code,
      tgt.source_partition_id           = src.source_partition_id,
      tgt.source_modified_ts            = src.source_modified_ts
    FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " AS tgt
    JOIN ",
        temp_table,
        " AS src
      ON  tgt.budget_skey = src.budget_skey
      AND tgt.project_skey = src.project_skey;
  "
      )
    )

    # Insert new rows not already in the target
    n_inserted <- dbExecute(
      con,
      paste0(
        "
    INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " (
        RefreshDate,
        project_skey,
        project_activity_skey,
        budget_skey,
        approved_by_skey,
        authorized_by_skey,
        submitted_by_skey,
        record_type,
        costtype,
        budget_desc,
        budget_item_skey,
        budget_item_id,
        approved_total_budget_value,
        approved_budget_capital_total,
        estimated_budget_capital,
        approved_budget_expense,
        estimated_budget_total_value,
        budget_capital_value,
        budget_status,
        budget_item_total_value,
        estimated_budget_uom,
        estimated_budget_quantity,
        budget_expense_amount,
        budget_notes,
        estimated_budget_expense,
        line_number,
        approved_budget_expense_total,
        estimated_budget_per_uom,
        edp_update_ts,
        source_unique_id,
        source_system_code,
        source_partition_id,
        source_modified_ts
    )
    SELECT
        src.RefreshDate,
        src.project_skey,
        src.project_activity_skey,
        src.budget_skey,
        src.approved_by_skey,
        src.authorized_by_skey,
        src.submitted_by_skey,
        src.record_type,
        src.costtype,
        src.budget_desc,
        src.budget_item_skey,
        src.budget_item_id,
        src.approved_total_budget_value,
        src.approved_budget_capital_total,
        src.estimated_budget_capital,
        src.approved_budget_expense,
        src.estimated_budget_total_value,
        src.budget_capital_value,
        src.budget_status,
        src.budget_item_total_value,
        src.estimated_budget_uom,
        src.estimated_budget_quantity,
        src.budget_expense_amount,
        src.budget_notes,
        src.estimated_budget_expense,
        src.line_number,
        src.approved_budget_expense_total,
        src.estimated_budget_per_uom,
        src.edp_update_ts,
        src.source_unique_id,
        src.source_system_code,
        src.source_partition_id,
        src.source_modified_ts
    FROM ",
        temp_table,
        " AS src
    LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " AS tgt
      ON tgt.budget_skey = src.budget_skey
      AND tgt.project_skey = src.project_skey
      WHERE tgt.budget_skey IS NULL AND tgt.project_skey IS NULL;
  "
      )
    )

    # Complete the transaction
    dbCommit(con)

    n_inserted <<- n_inserted
    n_updated <<- n_updated
    # Rollback transaction on failure
  },
  error = function(e) {
    dbRollback(con)
    stop(e)
  }
)
