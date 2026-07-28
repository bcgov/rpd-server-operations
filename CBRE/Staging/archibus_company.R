# For server logging
# Begin timer
task_start <- Sys.time()

# Setup necessary variables
ETL_STATUS <- "DEV"
SQL_SERVER <- if (ETL_STATUS == "PROD") {
  "dynamo.idir.bcgov\\CA_PRD"
} else {
  "windfarm.idir.bcgov\\CA_TST"
}
DB_NAME <- "BuildingIntelligence"
SCHEMA_NAME <- "CbreStaging"
TABLE_NAME <- "archibus_company"
CBRE_TABLE_NAME <- "archibus_company"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "archibus_company"

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
  start_time = etl_window$cbre_start_time,
  end_time = etl_window$cbre_end_time
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
  cond <- structure(
    class = c("no_data_condition", "condition"),
    list(message = no_data_msg)
  )
  stop(cond)
}

clean_data <- raw_data |>
  # clean_data <- company_raw_data |>
  purrr::pluck("data") |>
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  mutate(
    across(
      c(
        company_date_last_updated,
        edp_last_updated_timestamp,
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything()) |>
  select(
    RefreshDate,
    company_address1,
    company_city_id,
    company_county_id,
    company_state_id,
    company_ctry_id,
    company_zip,
    company_status_pobc,
    company_name,
    company_company,
    company_company_key,
    company_dv_id,
    company_dp_id,
    company_vendor,
    company_site_number,
    company_option1,
    company_regn_id,
    company_date_start_pobc,
    company_date_end_pobc,
    company_date_last_updated,
    company_eft,
    company_fax,
    company_phone,
    source_system,
    edp_last_updated_timestamp,
    edp_update_ts
  )


# Database Transaction ####
# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
        RefreshDate                   DATETIME2(3)  NOT NULL,
        company_address1              NVARCHAR(150) NULL,
        company_city_id               NVARCHAR(50)  NULL,
        company_county_id             NVARCHAR(50)  NULL,
        company_state_id              NVARCHAR(10)  NULL,
        company_ctry_id               NVARCHAR(10)  NULL,
        company_zip                   NVARCHAR(20)  NULL,
        company_status_pobc           NVARCHAR(20)  NULL,
        company_name                  NVARCHAR(200) NULL,
        company_company               NVARCHAR(50)  NOT NULL,
        company_company_key           NVARCHAR(50)  NULL,
        company_dv_id                 NVARCHAR(20)  NULL,
        company_dp_id                 NVARCHAR(50)  NULL,
        company_vendor                NVARCHAR(30)  NULL,
        company_site_number           NVARCHAR(30)  NULL,
        company_option1               NVARCHAR(30)  NULL,
        company_regn_id               NVARCHAR(30)  NULL,
        company_date_start_pobc       NVARCHAR(50)  NULL,
        company_date_end_pobc         NVARCHAR(50)  NULL,
        company_date_last_updated     DATETIME2(3)  NULL,
        company_eft                   NVARCHAR(20)  NULL,
        company_fax                   NVARCHAR(30)  NULL,
        company_phone                 NVARCHAR(50)  NULL,
        source_system                 NVARCHAR(20)  NULL,
        edp_last_updated_timestamp    DATETIME2(3)  NULL,
        edp_update_ts                 DATETIME2(3)  NOT NULL,

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
    CREATE TABLE ",
        SCHEMA_NAME,
        ".",
        TEMP_TABLE,
        " (
      RefreshDate                   DATETIME2(3)  NOT NULL,
      company_address1              NVARCHAR(150) NULL,
      company_city_id               NVARCHAR(50)  NULL,
      company_county_id             NVARCHAR(50)  NULL,
      company_state_id              NVARCHAR(10)  NULL,
      company_ctry_id               NVARCHAR(10)  NULL,
      company_zip                   NVARCHAR(20)  NULL,
      company_status_pobc           NVARCHAR(20)  NULL,
      company_name                  NVARCHAR(200) NULL,
      company_company               NVARCHAR(50)  NOT NULL,
      company_company_key           NVARCHAR(50)  NULL,
      company_dv_id                 NVARCHAR(20)  NULL,
      company_dp_id                 NVARCHAR(50)  NULL,
      company_vendor                NVARCHAR(30)  NULL,
      company_site_number           NVARCHAR(30)  NULL,
      company_option1               NVARCHAR(30)  NULL,
      company_regn_id               NVARCHAR(30)  NULL,
      company_date_start_pobc       NVARCHAR(50)  NULL,
      company_date_end_pobc         NVARCHAR(50)  NULL,
      company_date_last_updated     DATETIME2(3)  NULL,
      company_eft                   NVARCHAR(20)  NULL,
      company_fax                   NVARCHAR(30)  NULL,
      company_phone                 NVARCHAR(50)  NULL,
      source_system                 NVARCHAR(20)  NULL,
      edp_last_updated_timestamp    DATETIME2(3)  NULL,
      edp_update_ts                 DATETIME2(3)  NOT NULL,
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
        company_address1,
        company_city_id,
        company_county_id,
        company_state_id,
        company_ctry_id,
        company_zip,
        company_status_pobc,
        company_name,
        company_company,
        company_company_key,
        company_dv_id,
        company_dp_id,
        company_vendor,
        company_site_number,
        company_option1,
        company_regn_id,
        company_date_start_pobc,
        company_date_end_pobc,
        company_date_last_updated,
        company_eft,
        company_fax,
        company_phone,
        source_system,
        edp_last_updated_timestamp,
        edp_update_ts
      )
       SELECT * FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    # Complete the transaction
    dbCommit(con)

    # Hoist to main environment
    n_inserted <<- n_inserted
    cat("ETL complete — inserted:", n_inserted, "\n")
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
