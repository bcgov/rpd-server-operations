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
SCHEMA_NAME <- "InfBronze"
TABLE_NAME <- "archibus_company"
CBRE_TABLE_NAME <- "archibus_company"
PRIMARY_KEY <- c(
  "company_company_key"
)
TARGET_TABLE <- DBI::Id(schema = SCHEMA_NAME, table = TABLE_NAME)
TEMP_TABLE <- paste0("#", TABLE_NAME, "Temp")
API_NAME <- "CBRE"
SCRIPT_NAME <- if (ETL_STATUS == "PROD") {
  paste0("PROD-", TABLE_NAME)
} else {
  paste0("TEST-", TABLE_NAME)
}
BATCH_ID <- as.numeric(format(task_start, "%Y%m%d%H%M%S", tz = "UTC"))
AUDIT_TABLE <- DBI::Id(schema = "ServerLogs", table = SCHEMA_NAME)

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

EXCLUDED_FROM_HASH <- c(
  "company_company_key", # natural key itself — not a "value" to hash
  "md5_hash", # partner-supplied hash — not used
  "edp_last_updated_timestamp",
  "edp_update_ts",
  "source_system",
  "source_account_name"
)

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- glue::glue(
    "CREATE TABLE
    {SCHEMA_NAME}.{TABLE_NAME}
    (
      company_address1             VARCHAR(500) NULL,
      company_address2             VARCHAR(500) NULL,
      company_alt_fax              VARCHAR(100) NULL,
      company_alt_phone            VARCHAR(100) NULL,
      company_city_id              VARCHAR(100) NULL,
      company_comments             VARCHAR(1000) NULL,
      company_company              VARCHAR(100) NULL,
      company_company_key          VARCHAR(100) NOT NULL,
      company_county_id            VARCHAR(100) NULL,
      company_ctry_id              VARCHAR(50)  NULL,
      company_date_end_pobc        VARCHAR(30)  NULL,
      company_date_last_updated    VARCHAR(30)  NULL,
      company_date_start_pobc      VARCHAR(30)  NULL,
      company_dp_id                VARCHAR(100) NULL,
      company_dv_id                VARCHAR(50)  NULL,
      company_eft                  VARCHAR(50)  NULL,
      company_email                VARCHAR(200) NULL,
      company_fax                  VARCHAR(100) NULL,
      company_name                 VARCHAR(500) NULL,
      company_option1              VARCHAR(50)  NULL,
      company_option2              VARCHAR(50)  NULL,
      company_phone                VARCHAR(100) NULL,
      company_regn_id              VARCHAR(100) NULL,
      company_site_number          VARCHAR(50)  NULL,
      company_state_id             VARCHAR(50)  NULL,
      company_status_pobc          VARCHAR(50)  NULL,
      company_vendor               VARCHAR(100) NULL,
      company_web_url              VARCHAR(500) NULL,
      company_website              VARCHAR(500) NULL,
      company_zip                  VARCHAR(50)  NULL,
      md5_hash                     CHAR(32)     NULL,
      edp_last_updated_timestamp   VARCHAR(100) NULL,
      source_system                VARCHAR(50)  NULL,
      source_account_name          VARCHAR(50)  NULL,
      edp_update_ts                VARCHAR(30)  NULL,
      row_hash                     CHAR(32)     NOT NULL,
      bronze_batch_id              BIGINT       NOT NULL,
      bronze_load_ts               DATETIME2(0) NOT NULL,
      CONSTRAINT PK_bronze_company PRIMARY KEY CLUSTERED ({paste(PRIMARY_KEY, collapse = ', ')}, bronze_load_ts)
    );"
  )
  dbExecute(con, sql)
}

# Initial Setup ####
# data <- raw_data |>
#   purrr::pluck("data")
#
# test <- data |>
#   group_by(
#     company_company_key
#   ) |>
#   mutate(count = n()) |>
#   filter(count > 1)
#
# tracked_cols <- get_tracked_cols(data, EXCLUDED_FROM_HASH)
#
# hashed <- add_row_hash(data, PRIMARY_KEY, tracked_cols) |>
#   mutate(
#     bronze_load_ts = as.POSIXct(task_start, tz = "UTC"),
#     bronze_batch_id = BATCH_ID
#   )
# str(hashed, max.level = 2, vec.len = 0, list.len = Inf)
# max_char_lengths(hashed)
# DBI::dbAppendTable(con, TARGET_TABLE, hashed)

etl_error <- NULL

# Hash and Classify Data ####
tryCatch(
  {
    data <- raw_data |>
      purrr::pluck("data")

    tracked_cols <- get_tracked_cols(data, EXCLUDED_FROM_HASH)

    hashed <- data |>
      add_row_hash(PRIMARY_KEY, tracked_cols) |>
      mutate(
        bronze_load_ts = as.POSIXct(task_start, tz = "UTC"),
        bronze_batch_id = BATCH_ID
      )

    classified_data <- classify_incoming(
      hashed,
      con,
      SCHEMA_NAME,
      TABLE_NAME,
      PRIMARY_KEY
    )
  },
  error = function(e) {
    log_etl_error(
      api_name = API_NAME,
      script_name = SCRIPT_NAME,
      table_name = TABLE_NAME,
      step = "hash_classify",
      condition = e
    )
    etl_error <<- e
  }
)

# Check for Existing Data not in current API pull ####
if (is.null(etl_error)) {
  tryCatch(
    {
      missing <- find_missing_from_pull(
        hashed,
        con,
        SCHEMA_NAME,
        TABLE_NAME,
        PRIMARY_KEY,
        status_col = "company_status_pobc"
      )
    },
    error = function(e) {
      log_etl_error(
        api_name = API_NAME,
        script_name = SCRIPT_NAME,
        table_name = TABLE_NAME,
        step = "find_missing",
        condition = e
      )
      etl_error <<- e
    }
  )
}


if (is.null(etl_error)) {
  tryCatch(
    {
      log_row <- apply_hash_gate(
        con,
        classified_data,
        TARGET_TABLE,
        API_NAME,
        CBRE_TABLE_NAME,
        BATCH_ID,
        task_start
      )

      audit_row <- log_row |>
        mutate(
          Missing = nrow(missing),
          Duration = round(
            (interval(task_start, Sys.time()) / dseconds()),
            digits = 2
          )
        )

      DBI::dbAppendTable(con, AUDIT_TABLE, audit_row)

      cat(
        "ETL complete — Audit Row Written:",
        audit_row$New,
        " new, ",
        audit_row$Changed,
        " changed, and ",
        audit_row$Unchanged,
        " unchanged.",
        "\n"
      )
    },
    error = function(e) {
      log_etl_error(
        api_name = API_NAME,
        script_name = SCRIPT_NAME,
        table_name = TABLE_NAME,
        step = "apply_hash_gate",
        condition = e
      )
      etl_error <<- e
    }
  )
}

task_end <- Sys.time()
task_duration <- interval(task_start, task_end) / dseconds()


if (is.null(etl_error)) {
  log_daily_etl_run(
    api_name = API_NAME,
    script_name = SCRIPT_NAME,
    table_name = TABLE_NAME,
    duration = task_duration,
    status = "SUCCESS",
    n_inserted = audit_row$New,
    n_updated = audit_row$Changed,
    n_deleted = nrow(missing),
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
