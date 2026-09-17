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
SCHEMA_NAME <- "CbreSilver"
TABLE_NAME <- "pjm_report_project_role"
CBRE_TABLE_NAME <- "pjm_report_project_role_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- if (ETL_STATUS == "PROD") {
  paste0("PROD-", TABLE_NAME)
} else {
  paste0("TEST-", TABLE_NAME)
}

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
  # start_time = etl_window$cbre_start_time,
  start_time = "2020-01-01T00:00:00Z",
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
  # comment out these after initial data analysis as risk of
  # losing columns in small data loads
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  mutate(
    RefreshDate = as.POSIXct(Sys.time(), tz = "UTC"),
    .before = everything()
  ) |>
  mutate(
    across(
      c(
        edp_update_ts,
        edp_create_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  mutate(
    across(
      c(
        project_skey
      ),
      as.character
    )
  ) |>
  select(
    RefreshDate,
    project_skey,
    project_created_by,
    project_manager,
    assistant_project_manager,
    doa_approver_client,
    doa_approver_pjm_srpjm_pjm_director,
    doa_approver_srpjm_pjm_director,
    account_market_pjm_leader,
    contractor,
    architect,
    project_contact,
    system_contact,
    client_project_executive,
    cbre_project_executive,
    project_pending_with,
    edp_create_ts,
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
        RefreshDate                          DATETIME2(3)  NOT NULL,
        project_skey                         NVARCHAR(20)  NOT NULL,
        project_created_by                   NVARCHAR(100) NULL,
        project_manager                      NVARCHAR(100) NULL,
        assistant_project_manager            NVARCHAR(100) NULL,
        doa_approver_client                  NVARCHAR(100) NULL,
        doa_approver_pjm_srpjm_pjm_director  NVARCHAR(100) NULL,
        doa_approver_srpjm_pjm_director      NVARCHAR(100) NULL,
        account_market_pjm_leader            NVARCHAR(100) NULL,
        contractor                           NVARCHAR(100) NULL,
        architect                            NVARCHAR(100) NULL,
        project_contact                      NVARCHAR(100) NULL,
        system_contact                       NVARCHAR(100) NULL,
        client_project_executive             NVARCHAR(100) NULL,
        cbre_project_executive               NVARCHAR(100) NULL,
        project_pending_with                 NVARCHAR(100) NULL,
        source_client_name                   NVARCHAR(100) NULL,
        edp_create_ts                        DATETIME2(3)  NULL,
        edp_update_ts                        DATETIME2(3)  NULL
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
          RefreshDate                          DATETIME2(3)  NOT NULL,
          project_skey                         NVARCHAR(20)  NOT NULL,
          project_created_by                   NVARCHAR(100) NULL,
          project_manager                      NVARCHAR(100) NULL,
          assistant_project_manager            NVARCHAR(100) NULL,
          doa_approver_client                  NVARCHAR(100) NULL,
          doa_approver_pjm_srpjm_pjm_director  NVARCHAR(100) NULL,
          doa_approver_srpjm_pjm_director      NVARCHAR(100) NULL,
          account_market_pjm_leader            NVARCHAR(100) NULL,
          contractor                           NVARCHAR(100) NULL,
          architect                            NVARCHAR(100) NULL,
          project_contact                      NVARCHAR(100) NULL,
          system_contact                       NVARCHAR(100) NULL,
          client_project_executive             NVARCHAR(100) NULL,
          cbre_project_executive               NVARCHAR(100) NULL,
          project_pending_with                 NVARCHAR(100) NULL,
          source_client_name                   NVARCHAR(100) NULL,
          edp_create_ts                        DATETIME2(3)  NULL,
          edp_update_ts                        DATETIME2(3)  NULL
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
        " (
        RefreshDate,
        project_skey,
        project_created_by,
        project_manager,
        assistant_project_manager,
        doa_approver_client,
        doa_approver_pjm_srpjm_pjm_director,
        doa_approver_srpjm_pjm_director,
        account_market_pjm_leader,
        contractor,
        architect,
        project_contact,
        system_contact,
        client_project_executive,
        cbre_project_executive,
        project_pending_with,
        source_client_name,
        edp_create_ts,
        edp_update_ts
    )
    SELECT
        RefreshDate,
        project_skey,
        project_created_by,
        project_manager,
        assistant_project_manager,
        doa_approver_client,
        doa_approver_pjm_srpjm_pjm_director,
        doa_approver_srpjm_pjm_director,
        account_market_pjm_leader,
        contractor,
        architect,
        project_contact,
        system_contact,
        client_project_executive,
        cbre_project_executive,
        project_pending_with,
        source_client_name,
        edp_create_ts,
        edp_update_ts
    FROM ",
        TEMP_TABLE,
        ";"
      )
    )

    # Complete the transaction
    dbCommit(con)

    # Hoist counts to outer scope for logging
    n_inserted <<- n_inserted

    cat("ETL complete — inserted: ", n_inserted, "\n")
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
