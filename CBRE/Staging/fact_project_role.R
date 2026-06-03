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
TABLE_NAME <- "fact_project_role"
CBRE_TABLE_NAME <- "fact_project_role_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "fact_project_role"

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
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ")
    )
  ) |>
  mutate(
    across(
      c(
        project_skey,
        project_role_skey,
        contact_skey
      ),
      as.character
    )
  ) |>
  select(
    RefreshDate,
    edp_update_ts,
    project_skey,
    project_role_skey,
    contact_skey,
    source_unique_id,
    source_system_code
  )

# dbRemoveTable(con, TARGET_TABLE)

if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate         DATETIME2(3)  NOT NULL,
      edp_update_ts       DATETIME2(3)  NULL,
      project_skey        NVARCHAR(20)  NOT NULL,
      project_role_skey   NVARCHAR(20)  NOT NULL,
      contact_skey        NVARCHAR(30)  NULL,
      source_unique_id    NVARCHAR(100) NULL,
      source_system_code  NVARCHAR(50)  NULL
    );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####

etl_start_time <- Sys.time()

etl_error <- NULL

# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

# Begin error handling and rollback of transaction on failure
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
          RefreshDate         DATETIME2(3)  NOT NULL,
          edp_update_ts       DATETIME2(3)  NULL,
          project_skey        NVARCHAR(20)  NOT NULL,
          project_role_skey   NVARCHAR(20)  NOT NULL,
          contact_skey        NVARCHAR(30)  NULL,
          source_unique_id    NVARCHAR(100) NULL,
          source_system_code  NVARCHAR(50)  NULL
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

    # -- Guard: catch duplicate keys in source data before touching target --
    dup_count <- dbGetQuery(
      con,
      paste0(
        "SELECT COUNT(*) AS n
         FROM (
           SELECT project_skey, project_role_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY project_skey, project_role_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate project_skey & project_role_skey values detected in source data (",
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
           tgt.RefreshDate        = src.RefreshDate,
           tgt.edp_update_ts      = src.edp_update_ts,
           tgt.contact_skey       = src.contact_skey,
           tgt.source_unique_id   = src.source_unique_id,
           tgt.source_system_code = src.source_system_code
         FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
         INNER JOIN ",
        TEMP_TABLE,
        " src
           ON  tgt.project_skey      = src.project_skey
           AND tgt.project_role_skey = src.project_role_skey;"
      )
    )

    # -- Insert new rows not already in the target --
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
          edp_update_ts,
          project_skey,
          project_role_skey,
          contact_skey,
          source_unique_id,
          source_system_code
        )
        SELECT
          src.RefreshDate,
          src.edp_update_ts,
          src.project_skey,
          src.project_role_skey,
          src.contact_skey,
          src.source_unique_id,
          src.source_system_code
        FROM ",
        TEMP_TABLE,
        " src
        LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
          ON  tgt.project_skey      = src.project_skey
          AND tgt.project_role_skey = src.project_role_skey
        WHERE tgt.project_skey IS NULL
          AND tgt.project_role_skey IS NULL;
        "
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
