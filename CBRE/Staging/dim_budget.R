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
TABLE_NAME <- "dim_budget"
CBRE_TABLE_NAME <- "dim_budget_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "dim_budget"

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
        budget_date,
        budget_submitted_date,
        authorized_date,
        source_modified_ts,
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ")
    )
  ) |>
  mutate(
    across(
      c(
        budget_skey
      ),
      as.character
    )
  ) |>
  select(
    RefreshDate,
    budget_skey,
    budget_id,
    budget_number,
    budget_type,
    budget_subject,
    budget_approval_status,
    budget_date,
    budget_submitted_date,
    authorized_date,
    source_unique_id,
    source_partition_id,
    source_modified_ts,
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
      RefreshDate                 DATETIME2(3)   NOT NULL,
      budget_skey                 NVARCHAR(20)   NOT NULL,
      budget_id                   INT            NOT NULL,
      budget_number               NVARCHAR(10)   NULL,
      budget_type                 NVARCHAR(30)   NULL,
      budget_subject              NVARCHAR(150)  NULL,
      budget_approval_status      NVARCHAR(30)   NULL,
      budget_date                 DATETIME2(3)   NULL,
      budget_submitted_date       DATETIME2(3)   NULL,
      authorized_date             DATETIME2(3)   NULL,
      source_unique_id            NVARCHAR(30)   NULL,
      source_partition_id         NVARCHAR(30)   NULL,
      source_modified_ts          DATETIME2(3)   NULL,
      edp_update_ts               DATETIME2(3)   NULL
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
        budget_skey                 NVARCHAR(20)   NOT NULL,
        budget_id                   INT            NOT NULL,
        budget_number               NVARCHAR(10)   NULL,
        budget_type                 NVARCHAR(30)   NULL,
        budget_subject              NVARCHAR(150)  NULL,
        budget_approval_status      NVARCHAR(30)   NULL,
        budget_date                 DATETIME2(3)   NULL,
        budget_submitted_date       DATETIME2(3)   NULL,
        authorized_date             DATETIME2(3)   NULL,
        source_unique_id            NVARCHAR(30)   NOT NULL,
        source_partition_id         NVARCHAR(30)   NOT NULL,
        source_modified_ts          DATETIME2(3)   NULL,
        edp_update_ts               DATETIME2(3)   NULL
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
           SELECT budget_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY budget_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate budget_skey values detected in source data (",
        dup_count,
        " keys affected). Rolling back."
      ))
    }

    # Update existing rows in the target table that have changed
    n_updated <- dbExecute(
      con,
      paste0(
        "
    UPDATE tgt
      SET
        tgt.RefreshDate             = src.RefreshDate,
        tgt.budget_skey             = src.budget_skey,
        tgt.budget_id               = src.budget_id,
        tgt.budget_number           = src.budget_number,
        tgt.budget_type             = src.budget_type,
        tgt.budget_subject          = src.budget_subject,
        tgt.budget_approval_status  = src.budget_approval_status,
        tgt.budget_date             = src.budget_date,
        tgt.budget_submitted_date   = src.budget_submitted_date,
        tgt.authorized_date         = src.authorized_date,
        tgt.source_unique_id        = src.source_unique_id,
        tgt.source_partition_id     = src.source_partition_id,
        tgt.source_modified_ts      = src.source_modified_ts,
        tgt.edp_update_ts           = src.edp_update_ts
      FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      JOIN ",
        TEMP_TABLE,
        " src
        ON tgt.budget_skey = src.budget_skey;
      "
      )
    )

    # Insert new rows that don't exist in the SQL table
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
      budget_skey,
      budget_id,
      budget_number,
      budget_type,
      budget_subject,
      budget_approval_status,
      budget_date,
      budget_submitted_date,
      authorized_date,
      source_unique_id,
      source_partition_id,
      source_modified_ts,
      edp_update_ts
    )
    SELECT
      src.RefreshDate,
      src.budget_skey,
      src.budget_id,
      src.budget_number,
      src.budget_type,
      src.budget_subject,
      src.budget_approval_status,
      src.budget_date,
      src.budget_submitted_date,
      src.authorized_date,
      src.source_unique_id,
      src.source_partition_id,
      src.source_modified_ts,
      src.edp_update_ts
    FROM ",
        TEMP_TABLE,
        " src
    LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      ON tgt.budget_skey = src.budget_skey
    WHERE tgt.budget_skey IS NULL;
    "
      )
    )

    # Complete the transaction
    dbCommit(con)

    # Hoist counts to outer scope for logging
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
