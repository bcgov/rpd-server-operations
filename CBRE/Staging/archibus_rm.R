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
TABLE_NAME <- "archibus_rm"
CBRE_TABLE_NAME <- "archibus_rm"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "archibus_rm"

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
  filter(rm_status_pobc == "Active") |>
  select(
    edp_update_ts,
    rm_date_costs_end,
    rm_bl_id,
    rm_fl_id,
    rm_rm_id,
    rm_area,
    rm_area_alloc,
    rm_area_chargable,
    rm_area_comn,
    rm_area_comn_nocup,
    rm_area_manual,
    rm_area_unalloc,
    rm_rm_cat,
    rm_rm_type,
    rm_name,
    rm_prorate,
    rm_ls_id,
    rm_dv_id,
    rm_dp_id
  ) |>
  mutate(
    across(
      c(
        rm_area,
        rm_area_alloc,
        rm_area_chargable,
        rm_area_comn,
        rm_area_comn_nocup,
        rm_area_manual,
        rm_area_unalloc
      ),
      as.double
    )
  ) |>
  mutate(
    across(
      c(
        edp_update_ts,
        rm_date_costs_end
      ),
      as.POSIXct
    )
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
        RefreshDate           DATETIME2(3)    NOT NULL,
        edp_update_ts         DATETIME2(3)    NOT NULL,
        rm_date_costs_end     DATETIME2(3)    NULL,
        rm_bl_id              NVARCHAR(20)    NOT NULL,
        rm_fl_id              NVARCHAR(20)    NOT NULL,
        rm_rm_id              NVARCHAR(20)    NOT NULL,
        rm_area               DECIMAL(18,5)   NULL,
        rm_area_alloc         DECIMAL(18,5)   NULL,
        rm_area_chargable     DECIMAL(18,5)   NULL,
        rm_area_comn          DECIMAL(18,5)   NULL,
        rm_area_comn_nocup    DECIMAL(18,5)   NULL,
        rm_area_manual        DECIMAL(18,5)   NULL,
        rm_area_unalloc       DECIMAL(18,5)   NULL,
        rm_rm_cat             NVARCHAR(50)    NULL,
        rm_rm_type            NVARCHAR(50)    NULL,
        rm_name               NVARCHAR(150)   NULL,
        rm_prorate            NVARCHAR(20)    NULL,
        rm_ls_id              NVARCHAR(50)    NULL,
        rm_dv_id              NVARCHAR(20)    NULL,
        rm_dp_id              NVARCHAR(20)    NULL
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
        RefreshDate           DATETIME2(3)    NOT NULL,
        edp_update_ts         DATETIME2(3)    NOT NULL,
        rm_date_costs_end     DATETIME2(3)    NULL,
        rm_bl_id              NVARCHAR(20)    NOT NULL,
        rm_fl_id              NVARCHAR(20)    NOT NULL,
        rm_rm_id              NVARCHAR(20)    NOT NULL,
        rm_area               DECIMAL(18,5)   NULL,
        rm_area_alloc         DECIMAL(18,5)   NULL,
        rm_area_chargable     DECIMAL(18,5)   NULL,
        rm_area_comn          DECIMAL(18,5)   NULL,
        rm_area_comn_nocup    DECIMAL(18,5)   NULL,
        rm_area_manual        DECIMAL(18,5)   NULL,
        rm_area_unalloc       DECIMAL(18,5)   NULL,
        rm_rm_cat             NVARCHAR(50)    NULL,
        rm_rm_type            NVARCHAR(50)    NULL,
        rm_name               NVARCHAR(150)   NULL,
        rm_prorate            NVARCHAR(20)    NULL,
        rm_ls_id              NVARCHAR(50)    NULL,
        rm_dv_id              NVARCHAR(20)    NULL,
        rm_dp_id              NVARCHAR(20)    NULL
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
        rm_date_costs_end,
        rm_bl_id,
        rm_fl_id,
        rm_rm_id,
        rm_area,
        rm_area_alloc,
        rm_area_chargable,
        rm_area_comn,
        rm_area_comn_nocup,
        rm_area_manual,
        rm_area_unalloc,
        rm_rm_cat,
        rm_rm_type,
        rm_name,
        rm_prorate,
        rm_ls_id,
        rm_dv_id,
        rm_dp_id
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
