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
source(here::here("./utilities/R/api_helpers.R"))
source(here::here("./utilities/R/event_logger.R"))
source(here::here("./utilities/R/sql_helper_functions.R"))

ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "pjm_fact_budget"
CBRE_TABLE_NAME <- "pjm_fact_budget_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
etl_window <- get_etl_window()
API_NAME <- "CBRE"
SCRIPT_NAME <- "pjm_fact_project"


# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# Query API
raw_data <- call_cbre_api(
  CBRE_TABLE_NAME,
  start_time = etl_window$start_time,
  end_time = etl_window$end_time
)

clean_data <- raw_data |>
  purrr::pluck("data") |>
  # # comment out these after initial data analysis as risk of
  # # losing columns in small data loads
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  mutate(RefreshDate = as.POSIXct(Sys.time())) |>
  mutate(
    across(
      c(
        project_skey,
        project_activity_skey,
        budget_skey,
        budget_item_skey,
        submitted_by_skey,
        authorized_by_skey,
        approved_by_skey
      ),
      as.character
    )
  ) |>
  mutate(
    across(
      c(
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        approved_total_budget_value,
        estimated_budget_capital,
        estimated_budget_total_value,
        estimated_total_value,
        budget_capital_value,
        budget_expense_amount,
        budget_item_total_value,
        estimated_budget_expense,
        approved_budget_capital,
        estimated_budget_quantity,
        approved_budget_capital_total
      ),
      as.double
    )
  ) |>
  select(
    RefreshDate,
    project_skey,
    project_activity_skey,
    budget_skey,
    budget_item_skey,
    submitted_by_skey,
    authorized_by_skey,
    approved_by_skey,
    budget_item_id,
    budget_status,
    budget_desc,
    costtype,
    record_type,
    line_number,
    glcode,
    approved_total_budget_value,
    estimated_budget_capital,
    estimated_budget_total_value,
    estimated_total_value,
    budget_capital_value,
    budget_expense_amount,
    budget_item_total_value,
    estimated_budget_expense,
    approved_budget_capital,
    estimated_budget_uom,
    budget_notes,
    estimated_budget_per_uom,
    estimated_budget_quantity,
    approved_budget_capital_total,
    scope_of_work,
    source_unique_id,
    source_partition_id,
    source_system_code,
    edp_update_ts
  )

# dbRemoveTable(con, TARGET_TABLE)

if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate                   DATETIME2(3)  NOT NULL,
      project_skey                  NVARCHAR(30)  NOT NULL,
      project_activity_skey         NVARCHAR(30)  NULL,
      budget_skey                   NVARCHAR(30)  NOT NULL,
      budget_item_skey              NVARCHAR(30)  NOT NULL,
      submitted_by_skey             NVARCHAR(30)  NULL,
      authorized_by_skey            NVARCHAR(30)  NULL,
      approved_by_skey              NVARCHAR(30)  NULL,
      budget_item_id                INT           NULL,
      budget_status                 NVARCHAR(50)  NULL,
      budget_desc                   NVARCHAR(255) NULL,
      costtype                      NVARCHAR(50)  NULL,
      record_type                   NVARCHAR(50)  NULL,
      line_number                   NVARCHAR(20)  NULL,
      glcode                        NVARCHAR(50)  NULL,
      approved_total_budget_value   DECIMAL(18,4) NULL,
      estimated_budget_capital      DECIMAL(18,4) NULL,
      estimated_budget_total_value  DECIMAL(18,4) NULL,
      estimated_total_value         DECIMAL(18,4) NULL,
      budget_capital_value          DECIMAL(18,4) NULL,
      budget_expense_amount         DECIMAL(18,4) NULL,
      budget_item_total_value       DECIMAL(18,4) NULL,
      estimated_budget_expense      DECIMAL(18,4) NULL,
      approved_budget_capital       DECIMAL(18,4) NULL,
      estimated_budget_uom          NVARCHAR(20)  NULL,
      budget_notes                  NVARCHAR(MAX) NULL,
      estimated_budget_per_uom      NVARCHAR(50)  NULL,
      estimated_budget_quantity     DECIMAL(18,4) NULL,
      approved_budget_capital_total DECIMAL(18,4) NULL,
      scope_of_work                 NVARCHAR(MAX) NULL,
      source_unique_id              NVARCHAR(50)  NULL,
      source_partition_id           INT           NULL,
      source_system_code            NVARCHAR(50)  NULL,
      edp_update_ts                 DATETIME2(3)  NULL
    );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####

etl_start_time <- Sys.time()

etl_error <- NULL

dbBegin(con)

tryCatch(
  {
    if (dbExistsTable(con, TEMP_TABLE)) {
      dbRemoveTable(con, TEMP_TABLE)
    }

    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        TEMP_TABLE,
        " (
          RefreshDate                   DATETIME2(3)  NOT NULL,
          project_skey                  NVARCHAR(30)  NOT NULL,
          project_activity_skey         NVARCHAR(30)  NULL,
          budget_skey                   NVARCHAR(30)  NOT NULL,
          budget_item_skey              NVARCHAR(30)  NOT NULL,
          submitted_by_skey             NVARCHAR(30)  NULL,
          authorized_by_skey            NVARCHAR(30)  NULL,
          approved_by_skey              NVARCHAR(30)  NULL,
          budget_item_id                INT           NULL,
          budget_status                 NVARCHAR(50)  NULL,
          budget_desc                   NVARCHAR(255) NULL,
          costtype                      NVARCHAR(50)  NULL,
          record_type                   NVARCHAR(50)  NULL,
          line_number                   NVARCHAR(20)  NULL,
          glcode                        NVARCHAR(50)  NULL,
          approved_total_budget_value   DECIMAL(18,4) NULL,
          estimated_budget_capital      DECIMAL(18,4) NULL,
          estimated_budget_total_value  DECIMAL(18,4) NULL,
          estimated_total_value         DECIMAL(18,4) NULL,
          budget_capital_value          DECIMAL(18,4) NULL,
          budget_expense_amount         DECIMAL(18,4) NULL,
          budget_item_total_value       DECIMAL(18,4) NULL,
          estimated_budget_expense      DECIMAL(18,4) NULL,
          approved_budget_capital       DECIMAL(18,4) NULL,
          estimated_budget_uom          NVARCHAR(20)  NULL,
          budget_notes                  NVARCHAR(MAX) NULL,
          estimated_budget_per_uom      NVARCHAR(50)  NULL,
          estimated_budget_quantity     DECIMAL(18,4) NULL,
          approved_budget_capital_total DECIMAL(18,4) NULL,
          scope_of_work                 NVARCHAR(MAX) NULL,
          source_unique_id              NVARCHAR(50)  NULL,
          source_partition_id           INT           NULL,
          source_system_code            NVARCHAR(50)  NULL,
          edp_update_ts                 DATETIME2(3)  NULL
        );"
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = clean_data,
      append = TRUE,
      overwrite = FALSE
    )

    # -- Guard: catch duplicate composite keys in source data --
    dup_count <- dbGetQuery(
      con,
      paste0(
        "SELECT COUNT(*) AS n
         FROM (
           SELECT project_skey, budget_skey, budget_item_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY project_skey, budget_skey, budget_item_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate composite key values (project_skey + budget_skey + budget_item_skey) ",
        "detected in source data (",
        dup_count,
        " keys affected). Rolling back."
      ))
    }

    # -- Update matched rows --
    n_updated <- dbExecute(
      con,
      paste0(
        "UPDATE tgt
         SET
           tgt.RefreshDate                   = src.RefreshDate,
           tgt.project_activity_skey         = src.project_activity_skey,
           tgt.submitted_by_skey             = src.submitted_by_skey,
           tgt.authorized_by_skey            = src.authorized_by_skey,
           tgt.approved_by_skey              = src.approved_by_skey,
           tgt.budget_item_id                = src.budget_item_id,
           tgt.budget_status                 = src.budget_status,
           tgt.budget_desc                   = src.budget_desc,
           tgt.costtype                      = src.costtype,
           tgt.record_type                   = src.record_type,
           tgt.line_number                   = src.line_number,
           tgt.glcode                        = src.glcode,
           tgt.approved_total_budget_value   = src.approved_total_budget_value,
           tgt.estimated_budget_capital      = src.estimated_budget_capital,
           tgt.estimated_budget_total_value  = src.estimated_budget_total_value,
           tgt.estimated_total_value         = src.estimated_total_value,
           tgt.budget_capital_value          = src.budget_capital_value,
           tgt.budget_expense_amount         = src.budget_expense_amount,
           tgt.budget_item_total_value       = src.budget_item_total_value,
           tgt.estimated_budget_expense      = src.estimated_budget_expense,
           tgt.approved_budget_capital       = src.approved_budget_capital,
           tgt.estimated_budget_uom          = src.estimated_budget_uom,
           tgt.budget_notes                  = src.budget_notes,
           tgt.estimated_budget_per_uom      = src.estimated_budget_per_uom,
           tgt.estimated_budget_quantity     = src.estimated_budget_quantity,
           tgt.approved_budget_capital_total = src.approved_budget_capital_total,
           tgt.scope_of_work                 = src.scope_of_work,
           tgt.source_unique_id              = src.source_unique_id,
           tgt.source_partition_id           = src.source_partition_id,
           tgt.source_system_code            = src.source_system_code,
           tgt.edp_update_ts                 = src.edp_update_ts
         FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
         INNER JOIN ",
        TEMP_TABLE,
        " src
           ON  tgt.project_skey     = src.project_skey
           AND tgt.budget_skey      = src.budget_skey
           AND tgt.budget_item_skey = src.budget_item_skey;"
      )
    )

    # -- Insert new rows not already in the target --
    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " (
          RefreshDate,
          project_skey,
          project_activity_skey,
          budget_skey,
          budget_item_skey,
          submitted_by_skey,
          authorized_by_skey,
          approved_by_skey,
          budget_item_id,
          budget_status,
          budget_desc,
          costtype,
          record_type,
          line_number,
          glcode,
          approved_total_budget_value,
          estimated_budget_capital,
          estimated_budget_total_value,
          estimated_total_value,
          budget_capital_value,
          budget_expense_amount,
          budget_item_total_value,
          estimated_budget_expense,
          approved_budget_capital,
          estimated_budget_uom,
          budget_notes,
          estimated_budget_per_uom,
          estimated_budget_quantity,
          approved_budget_capital_total,
          scope_of_work,
          source_unique_id,
          source_partition_id,
          source_system_code,
          edp_update_ts
        )
        SELECT
          src.RefreshDate,
          src.project_skey,
          src.project_activity_skey,
          src.budget_skey,
          src.budget_item_skey,
          src.submitted_by_skey,
          src.authorized_by_skey,
          src.approved_by_skey,
          src.budget_item_id,
          src.budget_status,
          src.budget_desc,
          src.costtype,
          src.record_type,
          src.line_number,
          src.glcode,
          src.approved_total_budget_value,
          src.estimated_budget_capital,
          src.estimated_budget_total_value,
          src.estimated_total_value,
          src.budget_capital_value,
          src.budget_expense_amount,
          src.budget_item_total_value,
          src.estimated_budget_expense,
          src.approved_budget_capital,
          src.estimated_budget_uom,
          src.budget_notes,
          src.estimated_budget_per_uom,
          src.estimated_budget_quantity,
          src.approved_budget_capital_total,
          src.scope_of_work,
          src.source_unique_id,
          src.source_partition_id,
          src.source_system_code,
          src.edp_update_ts
        FROM ",
        TEMP_TABLE,
        " src
        LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
          ON  tgt.project_skey     = src.project_skey
          AND tgt.budget_skey      = src.budget_skey
          AND tgt.budget_item_skey = src.budget_item_skey
        WHERE tgt.project_skey IS NULL;"
      )
    )

    dbCommit(con)

    n_updated <<- n_updated
    n_inserted <<- n_inserted

    cat("ETL complete — updated:", n_updated, "| inserted:", n_inserted, "\n")
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
  }
)

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    status = "SUCCESS",
    n_inserted = n_inserted,
    n_updated = n_updated,
    n_deleted = NA,
    message = "ETL completed successfully"
  )
} else {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    status = "FAILURE",
    message = substr(etl_error$message, 1, 500)
  )
  stop(etl_error)
}
