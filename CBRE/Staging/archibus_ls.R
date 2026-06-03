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
TABLE_NAME <- "Leasing"
CBRE_TABLE_NAME <- "archibus_ls"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "Leasing"

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

if (is.null(raw_data$data) || nrow(raw_data$data) == 0) {
  cat(
    "No data returned from API for window",
    etl_window$start_time,
    "to",
    etl_window$end_time,
    "— nothing to load. Exiting gracefully.\n"
  )
  stop("No new data from API")
}

clean_data <- raw_data |>
  purrr::pluck("data") |>
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  select(
    edp_update_ts,
    ls_status,
    ls_ls_id,
    ls_bl_id,
    ls_pr_id,
    ls_ls_parent_id,
    ls_option1,
    ls_date_start,
    ls_date_end,
    ls_date_move,
    ls_date_terminated,
    ls_fasb_ls_type,
    ls_appropriated_hectares,
    ls_appropriated_parking_stalls,
    ls_area_negotiated,
    ls_appropriated_sqm,
    ls_area_common,
    ls_area_rentable,
    ls_area_usable,
    ls_lease_sublease,
    ls_multi_tenant,
    ls_non_standard,
    ls_project_id,
    ls_op_cost_type,
    ls_tn_name,
    ls_version
  ) |>
  mutate(
    across(
      c(
        ls_appropriated_hectares,
        ls_area_negotiated,
        ls_appropriated_parking_stalls,
        ls_appropriated_sqm,
        ls_area_common,
        ls_area_rentable,
        ls_area_usable
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        edp_update_ts,
        ls_date_start,
        ls_date_end,
        ls_date_move,
        ls_date_terminated
      ),
      as.POSIXct
    )
  ) |>
  mutate(
    ls_id_key = case_when(
      is.na(ls_bl_id) & !is.na(ls_pr_id) ~ ls_pr_id,
      .default = ls_bl_id
    ),
    .before = ls_ls_id
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything())

# Database Transaction ####
# dbRemoveTable(con, Id(schema = SCHEMA_NAME, table = TABLE_NAME))
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
        RefreshDate                    DATETIME2(3)   NOT NULL,
        edp_update_ts                  DATETIME2(3)   NOT NULL,
        ls_status                      NVARCHAR(30)   NULL,
        ls_id_key                      NVARCHAR(20)   NULL,
        ls_ls_id                       NVARCHAR(20)   NOT NULL,
        ls_bl_id                       NVARCHAR(20)   NULL,
        ls_pr_id                       NVARCHAR(20)   NULL,
        ls_ls_parent_id                NVARCHAR(20)   NULL,
        ls_option1                     NVARCHAR(20)   NULL,
        ls_date_start                  DATETIME2(3)   NULL,
        ls_date_end                    DATETIME2(3)   NULL,
        ls_date_move                   DATETIME2(3)   NULL,
        ls_date_terminated             DATETIME2(3)   NULL,
        ls_fasb_ls_type                NVARCHAR(30)   NULL,
        ls_appropriated_hectares       DECIMAL(18,5)  NULL,
        ls_appropriated_parking_stalls DECIMAL(18,5)  NULL,
        ls_area_negotiated             DECIMAL(18,5)  NULL,
        ls_appropriated_sqm            DECIMAL(18,5)  NULL,
        ls_area_common                 DECIMAL(18,5)  NULL,
        ls_area_rentable               DECIMAL(18,5)  NULL,
        ls_area_usable                 DECIMAL(18,5)  NULL,
        ls_lease_sublease              NVARCHAR(5)    NULL,
        ls_multi_tenant                NVARCHAR(5)    NULL,
        ls_non_standard                NVARCHAR(5)    NULL,
        ls_project_id                  NVARCHAR(30)   NULL,
        ls_op_cost_type                NVARCHAR(20)   NULL,
        ls_tn_name                     NVARCHAR(20)   NULL,
        ls_version                     NVARCHAR(5)    NULL
      );"
  )
  dbExecute(con, sql)
}

etl_error <- NULL

# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and roll back on transaction failure
tryCatch(
  {
    if (dbExistsTable(con, TEMP_TABLE)) {
      dbRemoveTable(con, TEMP_TABLE)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "
    CREATE TABLE  ",
        SCHEMA_NAME,
        ".",
        TEMP_TABLE,
        " (
        RefreshDate                    DATETIME2(3)   NOT NULL,
        edp_update_ts                  DATETIME2(3)   NOT NULL,
        ls_status                      NVARCHAR(30)   NULL,
        ls_id_key                      NVARCHAR(20)   NULL,
        ls_ls_id                       NVARCHAR(20)   NOT NULL,
        ls_bl_id                       NVARCHAR(20)   NULL,
        ls_pr_id                       NVARCHAR(20)   NULL,
        ls_ls_parent_id                NVARCHAR(20)   NULL,
        ls_option1                     NVARCHAR(20)   NULL,
        ls_date_start                  DATETIME2(3)   NULL,
        ls_date_end                    DATETIME2(3)   NULL,
        ls_date_move                   DATETIME2(3)   NULL,
        ls_date_terminated             DATETIME2(3)   NULL,
        ls_fasb_ls_type                NVARCHAR(30)   NULL,
        ls_appropriated_hectares       DECIMAL(18,5)  NULL,
        ls_appropriated_parking_stalls DECIMAL(18,5)  NULL,
        ls_area_negotiated             DECIMAL(18,5)  NULL,
        ls_appropriated_sqm            DECIMAL(18,5)  NULL,
        ls_area_common                 DECIMAL(18,5)  NULL,
        ls_area_rentable               DECIMAL(18,5)  NULL,
        ls_area_usable                 DECIMAL(18,5)  NULL,
        ls_lease_sublease              NVARCHAR(5)    NULL,
        ls_multi_tenant                NVARCHAR(5)    NULL,
        ls_non_standard                NVARCHAR(5)    NULL,
        ls_project_id                  NVARCHAR(30)   NULL,
        ls_op_cost_type                NVARCHAR(20)   NULL,
        ls_tn_name                     NVARCHAR(20)   NULL,
        ls_version                     NVARCHAR(5)    NULL
    );
  "
      )
    )

    dbWriteTable(
      con,
      name = TEMP_TABLE,
      value = clean_data,
      append = TRUE,
      overwrite = FALSE
    )

    dbExecute(
      con,
      paste0(
        "DELETE FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        ";"
      )
    )

    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        "(
        RefreshDate,
        edp_update_ts,
        ls_status,
        ls_id_key,
        ls_ls_id,
        ls_bl_id,
        ls_pr_id,
        ls_ls_parent_id,
        ls_option1,
        ls_date_start,
        ls_date_end,
        ls_date_move,
        ls_date_terminated,
        ls_fasb_ls_type,
        ls_appropriated_hectares,
        ls_appropriated_parking_stalls,
        ls_area_negotiated,
        ls_appropriated_sqm,
        ls_area_common,
        ls_area_rentable,
        ls_area_usable,
        ls_lease_sublease,
        ls_multi_tenant,
        ls_non_standard,
        ls_project_id,
        ls_op_cost_type,
        ls_tn_name,
        ls_version
      )
       SELECT * FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    # Complete the transaction
    dbCommit(con)

    n_inserted <<- n_inserted

    # Rollback transaction on failure
  },
  error = function(e) {
    dbRollback(con)
    etl_error <<- e
  }
)

task_end <- Sys.time()
task_duration <- interval(task_start, task_end) / dseconds()

if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = task_duration,
    status = "SUCCESS",
    n_inserted = n_inserted,
    n_updated = NA,
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
