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
TABLE_NAME <- "archibus_dp"
CBRE_TABLE_NAME <- "archibus_dp"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "archibus_dp"


# Connect to SQL database
con <- dbConnect(
  odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = SQL_SERVER,
  database = DB_NAME,
  Trusted_Connection = "Yes"
)

# call api
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
  purrr::pluck("data") |>
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  select(
    edp_update_ts,
    dp_name,
    dp_dp_id,
    dp_dv_id,
    dp_customer_category,
    dp_customer_segment,
    dp_gl_code,
    dp_hpattern_acad,
    dp_status,
    dp_tax_code,
    dp_area_chargable,
    dp_area_comn,
    dp_area_comn_nocup,
    dp_area_nocup,
    dp_area_ocup,
    dp_area_rm
  ) |>
  mutate(
    across(
      c(
        dp_area_chargable,
        dp_area_comn,
        dp_area_comn_nocup,
        dp_area_nocup,
        dp_area_ocup,
        dp_area_rm
      ),
      as.double
    )
  ) |>
  mutate(
    edp_update_ts = as.POSIXct(
      edp_update_ts,
      format = "%Y-%m-%dT%H:%M:%OSZ",
      tz = "UTC"
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
        RefreshDate            DATETIME2(3)   NOT NULL,
        edp_update_ts          DATETIME2(3)   NULL,
        dp_name                NVARCHAR(200)  NULL,
        dp_dp_id               NVARCHAR(50)   NOT NULL,
        dp_dv_id               NVARCHAR(10)   NULL,
        dp_customer_category   NVARCHAR(25)   NULL,
        dp_customer_segment    NVARCHAR(25)   NULL,
        dp_gl_code             NVARCHAR(50)   NULL,
        dp_hpattern_acad       NVARCHAR(50)   NULL,
        dp_status              NVARCHAR(10)   NULL,
        dp_tax_code            NVARCHAR(20)   NULL,
        dp_area_chargable      DECIMAL(18,2)  NULL,
        dp_area_comn           DECIMAL(18,2)  NULL,
        dp_area_comn_nocup     DECIMAL(18,2)  NULL,
        dp_area_nocup          DECIMAL(18,2)  NULL,
        dp_area_ocup           DECIMAL(18,2)  NULL,
        dp_area_rm             DECIMAL(18,2)  NULL
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
        RefreshDate            DATETIME2(3)   NOT NULL,
        edp_update_ts          DATETIME2(3)   NULL,
        dp_name                NVARCHAR(200)  NULL,
        dp_dp_id               NVARCHAR(50)   NOT NULL,
        dp_dv_id               NVARCHAR(10)   NULL,
        dp_customer_category   NVARCHAR(25)   NULL,
        dp_customer_segment    NVARCHAR(25)   NULL,
        dp_gl_code             NVARCHAR(50)   NULL,
        dp_hpattern_acad       NVARCHAR(50)   NULL,
        dp_status              NVARCHAR(10)   NULL,
        dp_tax_code            NVARCHAR(20)   NULL,
        dp_area_chargable      DECIMAL(18,2)  NULL,
        dp_area_comn           DECIMAL(18,2)  NULL,
        dp_area_comn_nocup     DECIMAL(18,2)  NULL,
        dp_area_nocup          DECIMAL(18,2)  NULL,
        dp_area_ocup           DECIMAL(18,2)  NULL,
        dp_area_rm             DECIMAL(18,2)  NULL
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
        dp_name,
        dp_dp_id,
        dp_dv_id,
        dp_customer_category,
        dp_customer_segment,
        dp_gl_code,
        dp_hpattern_acad,
        dp_status,
        dp_tax_code,
        dp_area_chargable,
        dp_area_comn,
        dp_area_comn_nocup,
        dp_area_nocup,
        dp_area_ocup,
        dp_area_rm
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
