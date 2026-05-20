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
TABLE_NAME <- "fact_project"
CBRE_TABLE_NAME <- "fact_project_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
etl_window <- get_etl_window()
API_NAME <- "CBRE"
SCRIPT_NAME <- "fact_project"

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
  start_time = paste0("2020-01-01T00:00:00Z"),
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
        edp_update_ts,
        charter_date,
        charter_date_approved_1,
        charter_date_approved_2,
        target_finish_date
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        estimated_budget,
        estimated_pjm_fees,
        gross_area,
        rentable_area,
        usable_area
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        project_skey,
        property_skey,
        building_use_type_skey,
        client_company_skey
      ),
      as.character
    )
  ) |>
  select(
    RefreshDate,
    edp_update_ts,
    project_skey,
    property_skey,
    project_phase,
    building_use_type_skey,
    client_company_skey,
    charter_date,
    charter_date_approved_1,
    charter_date_approved_2,
    target_finish_date,
    project_duration,
    estimated_pjm_fees,
    estimated_budget,
    gross_area,
    gross_area_uom,
    usable_area,
    usable_area_uom,
    rentable_area,
    rentable_area_uom,
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
      RefreshDate                 DATETIME2(3)   NOT NULL,
      edp_update_ts               DATETIME2(3)   NULL,
      project_skey                NVARCHAR(30)   NOT NULL,
      property_skey               NVARCHAR(30)   NULL,
      project_phase               NVARCHAR(100)  NULL,
      building_use_type_skey      NVARCHAR(30)   NULL,
      client_company_skey         NVARCHAR(30)   NULL,
      charter_date                DATETIME2(3)   NULL,
      charter_date_approved_1     DATETIME2(3)   NULL,
      charter_date_approved_2     DATETIME2(3)   NULL,
      target_finish_date          DATETIME2(3)   NULL,
      project_duration            NVARCHAR(50)   NULL,
      estimated_pjm_fees          DECIMAL(18,4)  NULL,
      estimated_budget            DECIMAL(18,4)  NULL,
      gross_area                  DECIMAL(18,4)  NULL,
      gross_area_uom              NVARCHAR(20)   NULL,
      usable_area                 DECIMAL(18,4)  NULL,
      usable_area_uom             NVARCHAR(20)   NULL,
      rentable_area               DECIMAL(18,4)  NULL,
      rentable_area_uom           NVARCHAR(20)   NULL,
      source_unique_id            NVARCHAR(30)   NULL,
      source_system_code          NVARCHAR(50)   NULL
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
        RefreshDate                 DATETIME2(3)   NOT NULL,
        edp_update_ts               DATETIME2(3)   NULL,
        project_skey                NVARCHAR(30)   NOT NULL,
        property_skey               NVARCHAR(30)   NULL,
        project_phase               NVARCHAR(100)  NULL,
        building_use_type_skey      NVARCHAR(30)   NULL,
        client_company_skey         NVARCHAR(30)   NULL,
        charter_date                DATETIME2(3)   NULL,
        charter_date_approved_1     DATETIME2(3)   NULL,
        charter_date_approved_2     DATETIME2(3)   NULL,
        target_finish_date          DATETIME2(3)   NULL,
        project_duration            NVARCHAR(50)   NULL,
        estimated_pjm_fees          DECIMAL(18,4)  NULL,
        estimated_budget            DECIMAL(18,4)  NULL,
        gross_area                  DECIMAL(18,4)  NULL,
        gross_area_uom              NVARCHAR(20)   NULL,
        usable_area                 DECIMAL(18,4)  NULL,
        usable_area_uom             NVARCHAR(20)   NULL,
        rentable_area               DECIMAL(18,4)  NULL,
        rentable_area_uom           NVARCHAR(20)   NULL,
        source_unique_id            NVARCHAR(30)   NULL,
        source_system_code          NVARCHAR(50)   NULL
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
           SELECT project_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY project_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate project_skey values detected in source data (",
        dup_count,
        " keys affected). Rolling back."
      ))
    }

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

    # Insert data into the SQL table
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
      property_skey,
      project_phase,
      building_use_type_skey,
      client_company_skey,
      charter_date,
      charter_date_approved_1,
      charter_date_approved_2,
      target_finish_date,
      project_duration,
      estimated_pjm_fees,
      estimated_budget,
      gross_area,
      gross_area_uom,
      usable_area,
      usable_area_uom,
      rentable_area,
      rentable_area_uom,
      source_unique_id,
      source_system_code
    )
    SELECT
      src.RefreshDate,
      src.edp_update_ts,
      src.project_skey,
      src.property_skey,
      src.project_phase,
      src.building_use_type_skey,
      src.client_company_skey,
      src.charter_date,
      src.charter_date_approved_1,
      src.charter_date_approved_2,
      src.target_finish_date,
      src.project_duration,
      src.estimated_pjm_fees,
      src.estimated_budget,
      src.gross_area,
      src.gross_area_uom,
      src.usable_area,
      src.usable_area_uom,
      src.rentable_area,
      src.rentable_area_uom,
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
      ON tgt.project_skey = src.project_skey
    WHERE tgt.project_skey IS NULL;
    "
      )
    )

    # Complete the transaction
    dbCommit(con)

    # Hoist counts to outer scope for logging
    n_inserted <<- n_inserted

    cat("ETL complete — inserted:", n_inserted, "\n")
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
