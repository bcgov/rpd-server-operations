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
TABLE_NAME <- "fm_benchmark_dim_asset"
CBRE_TABLE_NAME <- "fm_benchmark_dim_asset_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "fm_benchmark_dim_asset"

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

if (raw_data$status == "partial") {
  # True API/network failure
  error_msg <- paste0(
    "API extraction failed for table '",
    CBRE_TABLE_NAME,
    "' ",
    "(window ",
    etl_window$start_time,
    " to ",
    etl_window$end_time,
    "): ",
    raw_data$error
  )
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = as.numeric(difftime(Sys.time(), task_start, units = "secs")),
    status = "FAILURE",
    message = error_msg
  )
  stop(error_msg)
}

if (raw_data$status == "no_data") {
  # API succeeded, nothing to load
  no_data_msg <- paste0(
    "No data returned from API for window ",
    etl_window$start_time,
    " to ",
    etl_window$end_time
  )
  cat(no_data_msg, "— nothing to load. Exiting gracefully.\n")
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = as.numeric(difftime(Sys.time(), task_start, units = "secs")),
    status = "NO_DATA",
    message = no_data_msg
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
  select(
    asset_skey,
    asset_id,
    name,
    built_year,
    ownership,
    mr_task_count,
    ops_task_count,
    folder_name,
    hierarchy,
    applicable_hierarchy_levels,
    asset_type,
    ops_utility_count,
    cost_city,
    cost_state,
    edp_update_ts
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything()) |>
  mutate(
    across(
      c(
        asset_skey
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
  )

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
      RefreshDate                    DATETIME2(3)   NOT NULL,
      asset_skey                     NVARCHAR(50)   NOT NULL,
      asset_id                       NVARCHAR(50)   NOT NULL,
      name                           NVARCHAR(50)   NULL,
      built_year                     INT            NULL,
      ownership                      NVARCHAR(50)   NULL,
      mr_task_count                  INT            NULL,
      ops_task_count                 INT            NULL,
      folder_name                    NVARCHAR(100)  NULL,
      hierarchy                      NVARCHAR(250)  NULL,
      applicable_hierarchy_levels    NVARCHAR(250)  NULL,
      asset_type                     NVARCHAR(100)  NULL,
      ops_utility_count              INT            NULL,
      cost_city                      NVARCHAR(100)  NULL,
      cost_state                     NVARCHAR(50)   NULL,
      edp_update_ts                  DATETIME2(3)   NULL
    );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####
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
          RefreshDate                    DATETIME2(3)   NOT NULL,
          asset_skey                     NVARCHAR(50)   NOT NULL,
          asset_id                       NVARCHAR(50)   NOT NULL,
          name                           NVARCHAR(50)   NULL,
          built_year                     INT            NULL,
          ownership                      NVARCHAR(50)   NULL,
          mr_task_count                  INT            NULL,
          ops_task_count                 INT            NULL,
          folder_name                    NVARCHAR(100)  NULL,
          hierarchy                      NVARCHAR(250)  NULL,
          applicable_hierarchy_levels    NVARCHAR(250)  NULL,
          asset_type                     NVARCHAR(100)  NULL,
          ops_utility_count              INT            NULL,
          cost_city                      NVARCHAR(100)  NULL,
          cost_state                     NVARCHAR(50)   NULL,
          edp_update_ts                  DATETIME2(3)   NULL
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
           SELECT asset_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY asset_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate asset_skey values detected in source data (",
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
           tgt.RefreshDate                 = src.RefreshDate,
           tgt.asset_id                    = src.asset_id,
           tgt.name                        = src.name,
           tgt.built_year                  = src.built_year,
           tgt.ownership                   = src.ownership,
           tgt.mr_task_count               = src.mr_task_count,
           tgt.ops_task_count              = src.ops_task_count,
           tgt.folder_name                 = src.folder_name,
           tgt.hierarchy                   = src.hierarchy,
           tgt.applicable_hierarchy_levels = src.applicable_hierarchy_levels,
           tgt.asset_type                  = src.asset_type,
           tgt.ops_utility_count           = src.ops_utility_count,
           tgt.cost_city                   = src.cost_city,
           tgt.cost_state                  = src.cost_state,
           tgt.edp_update_ts               = src.edp_update_ts
         FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
         INNER JOIN ",
        TEMP_TABLE,
        " src
           ON tgt.asset_skey = src.asset_skey;"
      )
    )

    # -- Insert new rows --
    n_inserted <- dbExecute(
      con,
      paste0(
        "INSERT INTO ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " (
          RefreshDate,
          asset_skey,
          asset_id,
          name,
          built_year,
          ownership,
          mr_task_count,
          ops_task_count,
          folder_name,
          hierarchy,
          applicable_hierarchy_levels,
          asset_type,
          ops_utility_count,
          cost_city,
          cost_state,
          edp_update_ts
        )
        SELECT
          src.RefreshDate,
          src.asset_skey,
          src.asset_id,
          src.name,
          src.built_year,
          src.ownership,
          src.mr_task_count,
          src.ops_task_count,
          src.folder_name,
          src.hierarchy,
          src.applicable_hierarchy_levels,
          src.asset_type,
          src.ops_utility_count,
          src.cost_city,
          src.cost_state,
          src.edp_update_ts
        FROM ",
        TEMP_TABLE,
        " src
        LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
          ON tgt.asset_skey = src.asset_skey
        WHERE tgt.asset_skey IS NULL;"
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
