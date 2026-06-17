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
TABLE_NAME <- "archibus_rmpct"
CBRE_TABLE_NAME <- "archibus_rmpct"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "archibus_rmpct"

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
  purrr::pluck("data") |>
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  filter(rmpct_status_pobc == "Active") |>
  select(
    edp_update_ts,
    rmpct_date_start,
    rmpct_date_created,
    rmpct_date_end,
    rmpct_bl_id,
    rmpct_fl_id,
    rmpct_rm_id,
    rmpct_area_chargable,
    rmpct_area_rm,
    rmpct_area_comn,
    rmpct_area_comn_nocup,
    rmpct_pct_space,
    rmpct_prorate,
    rmpct_status,
    rmpct_rm_cat,
    rmpct_rm_type,
    rmpct_ls_id,
    rmpct_dv_id,
    rmpct_dp_id
  ) |>
  mutate(across(
    c(
      rmpct_area_chargable,
      rmpct_area_rm,
      rmpct_area_comn,
      rmpct_area_comn_nocup,
      rmpct_pct_space
    ),
    as.double
  )) |>
  mutate(across(
    c(
      edp_update_ts,
      rmpct_date_start,
      rmpct_date_created,
      rmpct_date_end
    ),
    as.POSIXct
  )) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything())

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- paste0(
    "CREATE TABLE ",
    SCHEMA_NAME,
    ".",
    TABLE_NAME,
    " (
        RefreshDate              DATETIME2(3)    NOT NULL,
        edp_update_ts            DATETIME2(3)    NOT NULL,
        rmpct_date_start         DATETIME2(3)    NULL,
        rmpct_date_created       DATETIME2(3)    NOT NULL,
        rmpct_date_end           DATETIME2(3)    NULL,
        rmpct_bl_id              NVARCHAR(20)    NOT NULL,
        rmpct_fl_id              NVARCHAR(10)    NOT NULL,
        rmpct_rm_id              NVARCHAR(20)    NOT NULL,
        rmpct_area_chargable     DECIMAL(18,5)   NULL,
        rmpct_area_rm            DECIMAL(18,5)   NULL,
        rmpct_area_comn          DECIMAL(18,5)   NULL,
        rmpct_area_comn_nocup    DECIMAL(18,5)   NULL,
        rmpct_pct_space          DECIMAL(9,5)    NULL,
        rmpct_prorate            NVARCHAR(20)    NULL,
        rmpct_status             NVARCHAR(10)    NULL,
        rmpct_rm_cat             NVARCHAR(50)    NULL,
        rmpct_rm_type            NVARCHAR(30)    NULL,
        rmpct_ls_id              NVARCHAR(50)    NULL,
        rmpct_dv_id              NVARCHAR(20)    NULL,
        rmpct_dp_id              NVARCHAR(20)    NULL
      );"
  )
  dbExecute(con, sql)
}

# Database Transaction ####
etl_error <- NULL

# Control database transaction to ensure all steps done together or not at all
dbBegin(con)

tryCatch(
  {
    if (dbExistsTable(con, TEMP_TABLE)) {
      dbRemoveTable(con, TEMP_TABLE)
    }

    # Create temp table to hold new data
    dbExecute(
      con,
      paste0(
        "CREATE TABLE ",
        TEMP_TABLE,
        " (
          RefreshDate              DATETIME2(3)    NOT NULL,
          edp_update_ts            DATETIME2(3)    NOT NULL,
          rmpct_date_start         DATETIME2(3)    NULL,
          rmpct_date_created       DATETIME2(3)    NOT NULL,
          rmpct_date_end           DATETIME2(3)    NULL,
          rmpct_bl_id              NVARCHAR(20)    NOT NULL,
          rmpct_fl_id              NVARCHAR(10)    NOT NULL,
          rmpct_rm_id              NVARCHAR(20)    NOT NULL,
          rmpct_area_chargable     DECIMAL(18,5)   NULL,
          rmpct_area_rm            DECIMAL(18,5)   NULL,
          rmpct_area_comn          DECIMAL(18,5)   NULL,
          rmpct_area_comn_nocup    DECIMAL(18,5)   NULL,
          rmpct_pct_space          DECIMAL(9,5)    NULL,
          rmpct_prorate            NVARCHAR(20)    NULL,
          rmpct_status             NVARCHAR(10)    NULL,
          rmpct_rm_cat             NVARCHAR(50)    NULL,
          rmpct_rm_type            NVARCHAR(30)    NULL,
          rmpct_ls_id              NVARCHAR(50)    NULL,
          rmpct_dv_id              NVARCHAR(20)    NULL,
          rmpct_dp_id              NVARCHAR(20)    NULL
          );"
      )
    )

    # Write the current tibble into the temp table
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
        rmpct_date_start,
        rmpct_date_created,
        rmpct_date_end,
        rmpct_bl_id,
        rmpct_fl_id,
        rmpct_rm_id,
        rmpct_area_chargable,
        rmpct_area_rm,
        rmpct_area_comn,
        rmpct_area_comn_nocup,
        rmpct_pct_space,
        rmpct_prorate,
        rmpct_status,
        rmpct_rm_cat,
        rmpct_rm_type,
        rmpct_ls_id,
        rmpct_dv_id,
        rmpct_dp_id
      )
       SELECT * FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    # Complete the transaction
    dbCommit(con)

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
