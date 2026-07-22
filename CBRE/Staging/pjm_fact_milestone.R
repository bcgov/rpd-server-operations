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
TABLE_NAME <- "pjm_fact_milestone"
CBRE_TABLE_NAME <- "pjm_fact_milestone_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "pjm_fact_milestone"

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
  # select_if(~ !all(is.na(.))) |>
  # select_if(~ !all(. == 0)) |>
  # select_if(~ !all(. == '-1')) |>
  # select_if(~ !all(. == "N/A")) |>
  # select_if(~ !all(. == "-")) |>
  mutate(RefreshDate = as.POSIXct(Sys.time()), .before = everything()) |>
  mutate(
    across(
      c(
        project_skey,
        milestone_skey,
        milestone_id,
        parent_milestone_id,
        responsible_contact_skey
      ),
      as.character
    )
  ) |>
  mutate(
    across(
      c(
        source_modified_ts,
        source_created_ts,
        edp_update_ts,
        estimated_start_date,
        revised_start_date,
        actual_start_date,
        estimated_end_date,
        revised_end_date,
        actual_end_date
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    )
  ) |>
  select(
    RefreshDate,
    project_skey,
    milestone_skey,
    milestone_id,
    parent_milestone_id,
    milestone_desc,
    milestone_notes,
    estimated_start_date,
    revised_start_date,
    actual_start_date,
    estimated_end_date,
    revised_end_date,
    actual_end_date,
    project_start_milestone_f,
    project_end_milestone_f,
    is_na_f,
    show_on_dashboard_f,
    responsible_contact_skey,
    serial_number,
    source_partition_id,
    source_modified_ts,
    source_created_ts,
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
      RefreshDate               DATETIME2(3)   NOT NULL,
      project_skey              NVARCHAR(20)   NOT NULL,
      milestone_skey            NVARCHAR(20)   NOT NULL,
      milestone_id              NVARCHAR(20)   NOT NULL,
      parent_milestone_id       NVARCHAR(20)   NULL,
      milestone_desc            NVARCHAR(500)  NULL,
      milestone_notes           NVARCHAR(1000) NULL,
      estimated_start_date      DATETIME2(3)   NULL,
      revised_start_date        DATETIME2(3)   NULL,
      actual_start_date         DATETIME2(3)   NULL,
      estimated_end_date        DATETIME2(3)   NULL,
      revised_end_date          DATETIME2(3)   NULL,
      actual_end_date           DATETIME2(3)   NULL,
      project_start_milestone_f NVARCHAR(1)    NULL,
      project_end_milestone_f   NVARCHAR(1)    NULL,
      is_na_f                   NVARCHAR(1)    NULL,
      show_on_dashboard_f       NVARCHAR(1)    NULL,
      responsible_contact_skey  NVARCHAR(20)   NULL,
      serial_number             NVARCHAR(10)   NULL,
      source_partition_id       INT            NOT NULL,
      source_modified_ts        DATETIME2(3)   NULL,
      source_created_ts         DATETIME2(3)   NULL,
      edp_update_ts             DATETIME2(3)   NULL
    );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####
# Control database transaction to ensure all steps done together or not at all
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
        RefreshDate               DATETIME2(3)   NOT NULL,
        project_skey              NVARCHAR(20)   NOT NULL,
        milestone_skey            NVARCHAR(20)   NOT NULL,
        milestone_id              NVARCHAR(20)   NOT NULL,
        parent_milestone_id       NVARCHAR(20)   NULL,
        milestone_desc            NVARCHAR(500)  NULL,
        milestone_notes           NVARCHAR(1000) NULL,
        estimated_start_date      DATETIME2(3)   NULL,
        revised_start_date        DATETIME2(3)   NULL,
        actual_start_date         DATETIME2(3)   NULL,
        estimated_end_date        DATETIME2(3)   NULL,
        revised_end_date          DATETIME2(3)   NULL,
        actual_end_date           DATETIME2(3)   NULL,
        project_start_milestone_f NVARCHAR(1)    NULL,
        project_end_milestone_f   NVARCHAR(1)    NULL,
        is_na_f                   NVARCHAR(1)    NULL,
        show_on_dashboard_f       NVARCHAR(1)    NULL,
        responsible_contact_skey  NVARCHAR(20)   NULL,
        serial_number             NVARCHAR(10)   NULL,
        source_partition_id       INT            NOT NULL,
        source_modified_ts        DATETIME2(3)   NULL,
        source_created_ts         DATETIME2(3)   NULL,
        edp_update_ts             DATETIME2(3)   NULL
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
           SELECT project_skey, milestone_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY project_skey, milestone_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate project_skey, milestone_skey values detected in source data (",
        dup_count,
        " keys affected). Rolling back."
      ))
    }

    # -- Update matched rows --
    n_updated <- dbExecute(
      con,
      paste0(
        "
      UPDATE tgt
      SET
        tgt.RefreshDate               = src.RefreshDate,
        tgt.milestone_id              = src.milestone_id,
        tgt.parent_milestone_id       = src.parent_milestone_id,
        tgt.milestone_desc            = src.milestone_desc,
        tgt.milestone_notes           = src.milestone_notes,
        tgt.estimated_start_date      = src.estimated_start_date,
        tgt.revised_start_date        = src.revised_start_date,
        tgt.actual_start_date         = src.actual_start_date,
        tgt.estimated_end_date        = src.estimated_end_date,
        tgt.revised_end_date          = src.revised_end_date,
        tgt.actual_end_date           = src.actual_end_date,
        tgt.project_start_milestone_f = src.project_start_milestone_f,
        tgt.project_end_milestone_f   = src.project_end_milestone_f,
        tgt.is_na_f                   = src.is_na_f,
        tgt.show_on_dashboard_f       = src.show_on_dashboard_f,
        tgt.responsible_contact_skey  = src.responsible_contact_skey,
        tgt.serial_number             = src.serial_number,
        tgt.source_partition_id       = src.source_partition_id,
        tgt.source_modified_ts        = src.source_modified_ts,
        tgt.source_created_ts         = src.source_created_ts,
        tgt.edp_update_ts             = src.edp_update_ts
      FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      JOIN ",
        TEMP_TABLE,
        " src
        ON  tgt.project_skey   = src.project_skey
        AND tgt.milestone_skey = src.milestone_skey;
      "
      )
    )

    # -- Insert new rows --
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
        project_skey,
        milestone_skey,
        milestone_id,
        parent_milestone_id,
        milestone_desc,
        milestone_notes,
        estimated_start_date,
        revised_start_date,
        actual_start_date,
        estimated_end_date,
        revised_end_date,
        actual_end_date,
        project_start_milestone_f,
        project_end_milestone_f,
        is_na_f,
        show_on_dashboard_f,
        responsible_contact_skey,
        serial_number,
        source_partition_id,
        source_modified_ts,
        source_created_ts,
        edp_update_ts
      )
      SELECT
        src.RefreshDate,
        src.project_skey,
        src.milestone_skey,
        src.milestone_id,
        src.parent_milestone_id,
        src.milestone_desc,
        src.milestone_notes,
        src.estimated_start_date,
        src.revised_start_date,
        src.actual_start_date,
        src.estimated_end_date,
        src.revised_end_date,
        src.actual_end_date,
        src.project_start_milestone_f,
        src.project_end_milestone_f,
        src.is_na_f,
        src.show_on_dashboard_f,
        src.responsible_contact_skey,
        src.serial_number,
        src.source_partition_id,
        src.source_modified_ts,
        src.source_created_ts,
        src.edp_update_ts
      FROM ",
        TEMP_TABLE,
        " src
      LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
        ON  tgt.project_skey   = src.project_skey
        AND tgt.milestone_skey = src.milestone_skey
      WHERE tgt.project_skey IS NULL
      AND tgt.milestone_skey IS NULL;
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
