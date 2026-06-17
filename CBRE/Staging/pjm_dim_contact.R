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
TABLE_NAME <- "pjm_dim_contact"
CBRE_TABLE_NAME <- "pjm_dim_contact_vw"
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- "pjm_dim_contact"

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
        contact_skey
      ),
      as.character
    )
  ) |>
  mutate(
    across(
      c(
        edp_update_ts
      ),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%OSZ")
    )
  ) |>
  select(
    RefreshDate,
    contact_skey,
    first_name,
    last_name,
    email_id,
    address_line_1,
    address_line_2,
    postal_code,
    state,
    country,
    company_name,
    job_title,
    contact_id,
    source_unique_id,
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
      RefreshDate         DATETIME2(3)   NOT NULL,
      contact_skey        NVARCHAR(20)   NOT NULL,
      first_name          NVARCHAR(100)  NULL,
      last_name           NVARCHAR(100)  NULL,
      email_id            NVARCHAR(100)  NULL,
      address_line_1      NVARCHAR(100)  NULL,
      address_line_2      NVARCHAR(100)  NULL,
      postal_code         NVARCHAR(100)  NULL,
      state               NVARCHAR(100)  NULL,
      country             NVARCHAR(100)  NULL,
      company_name        NVARCHAR(100)  NULL,
      job_title           NVARCHAR(100)  NULL,
      contact_id          NVARCHAR(100)  NULL,
      source_unique_id    NVARCHAR(100)  NULL,
      edp_update_ts       DATETIME2(3)   NULL
    );"
  )

  dbExecute(con, sql)
}

# Database Transaction ####
etl_start_time <- Sys.time()

etl_error <- NULL

# Control database transaction to ensure all steps done together or not at all
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
        RefreshDate         DATETIME2(3)   NOT NULL,
        contact_skey        NVARCHAR(20)   NOT NULL,
        first_name          NVARCHAR(100)  NULL,
        last_name           NVARCHAR(100)  NULL,
        email_id            NVARCHAR(100)  NULL,
        address_line_1      NVARCHAR(100)  NULL,
        address_line_2      NVARCHAR(100)  NULL,
        postal_code         NVARCHAR(100)  NULL,
        state               NVARCHAR(100)  NULL,
        country             NVARCHAR(100)  NULL,
        company_name        NVARCHAR(100)  NULL,
        job_title           NVARCHAR(100)  NULL,
        contact_id          NVARCHAR(100)  NULL,
        source_unique_id    NVARCHAR(100)  NULL,
        edp_update_ts       DATETIME2(3)   NULL
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

    # -- Guard: catch duplicate composite keys in source data --
    dup_count <- dbGetQuery(
      con,
      paste0(
        "SELECT COUNT(*) AS n
         FROM (
           SELECT contact_skey
           FROM ",
        TEMP_TABLE,
        "
           GROUP BY contact_skey
           HAVING COUNT(*) > 1
         ) dupes;"
      )
    )$n

    if (dup_count > 0) {
      stop(paste0(
        "Duplicate contact_skey key values ",
        "detected in source data (",
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
        tgt.RefreshDate       = src.RefreshDate,
        tgt.contact_skey      = src.contact_skey,
        tgt.first_name        = src.first_name,
        tgt.last_name         = src.last_name,
        tgt.email_id          = src.email_id,
        tgt.address_line_1    = src.address_line_1,
        tgt.address_line_2    = src.address_line_2,
        tgt.postal_code       = src.postal_code,
        tgt.state             = src.state,
        tgt.country           = src.country,
        tgt.company_name      = src.company_name,
        tgt.job_title         = src.job_title,
        tgt.contact_id        = src.contact_id,
        tgt.source_unique_id  = src.source_unique_id
      FROM ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
      JOIN ",
        TEMP_TABLE,
        " src
        ON  tgt.contact_skey = src.contact_skey;
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
        contact_skey,
        first_name,
        last_name,
        email_id,
        address_line_1,
        address_line_2,
        postal_code,
        state,
        country,
        company_name,
        job_title,
        contact_id,
        source_unique_id
      )
      SELECT
        src.RefreshDate,
        src.contact_skey,
        src.first_name,
        src.last_name,
        src.email_id,
        src.address_line_1,
        src.address_line_2,
        src.postal_code,
        src.state,
        src.country,
        src.company_name,
        src.job_title,
        src.contact_id,
        src.source_unique_id
      FROM ",
        TEMP_TABLE,
        " src
      LEFT JOIN ",
        SCHEMA_NAME,
        ".",
        TABLE_NAME,
        " tgt
        ON  tgt.contact_skey = src.contact_skey
      WHERE tgt.contact_skey IS NULL;
      "
      )
    )

    # Complete the transaction
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
