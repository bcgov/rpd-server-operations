#' @title Archibus Project Team Member
#'
#' @description
#' Loads project team membership records from the CBRE API into the Bronze
#' layer. The script extracts project-team assignments and contact
#' information, computes row-level hashes to detect changes, classifies
#' incoming records as new, changed, or unchanged, and appends historical
#' versions to support Bronze layer auditing and change tracking.
#'
#' @database BuildingIntelligence
#' @schema InfBronze
#' @table archibus_projteam
#'
#' @source CBRE API archibus_projteam entity
#'
#' @load_type Incremental append with hash-based change detection
#' @grain One row per project-team-member assignment version per bronze load timestamp
#'
#' @filters Records are retrieved only for the configured ETL extraction window
#' @filters Technical metadata fields are excluded from row-hash calculations
#'   to avoid false-positive change detection
#' @filters Missing records from the current source extract are identified and
#'   logged separately for downstream processing
#'
#' @consumers TODO
#'
#' @column projteam_project_id Associated project identifier
#' @column projteam_member_id Project team member identifier
#' @column projteam_member_type Project role or member classification
#' @column projteam_name_first Team member first name
#' @column projteam_name_last Team member last name
#' @column projteam_email Team member email address
#' @column row_hash MD5 hash of tracked business attributes used for change detection
#' @column bronze_batch_id ETL batch identifier for the load execution
#' @column bronze_load_ts UTC timestamp when the Bronze record version was loaded
#'
#' @notes
#' Data is sourced from the CBRE API rather than directly from a database
#' table.
#'
#' This entity links projects to assigned team members and stores associated
#' contact information, organizational affiliation, member role, and project
#' participation details.
#'
#' A hash-gating process classifies rows as New, Changed, or Unchanged before
#' loading. Historical versions are retained through the composite primary key
#' of (projteam_project_id, projteam_member_id, bronze_load_ts).
#'
#' Missing records from the source extract are identified during processing and
#' recorded for downstream auditing and soft-delete workflows.
#'
#' @owner David Rattray
#' @last_reviewed 2026-11-23

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
TABLE_NAME <- "archibus_projteam"
CBRE_TABLE_NAME <- "archibus_projteam"
PRIMARY_KEY <- c("projteam_project_id", "projteam_member_id")
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
  "projteam_project_id", # composite key value — not a "value" to hash
  "projteam_member_id", # composite key value — not a "value" to hash
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
      projteam_address1             VARCHAR(250)  NULL,
      projteam_address2             VARCHAR(100)  NULL,
      projteam_bl_id                VARCHAR(100)  NULL,
      projteam_city_id              VARCHAR(100)  NULL,
      projteam_company              VARCHAR(100)  NULL,
      projteam_county_id            VARCHAR(100)  NULL,
      projteam_ctry_id              VARCHAR(50)   NULL,
      projteam_email                VARCHAR(100)  NULL,
      projteam_fax                  VARCHAR(100)  NULL,
      projteam_honorific            VARCHAR(50)   NULL,
      projteam_image_file           VARCHAR(250)  NULL,
      projteam_member_id            VARCHAR(100)  NOT NULL,
      projteam_member_type          VARCHAR(100)  NULL,
      projteam_name_first           VARCHAR(100)  NULL,
      projteam_name_last            VARCHAR(100)  NULL,
      projteam_notes                VARCHAR(500)  NULL,
      projteam_option1              VARCHAR(1000) NULL,
      projteam_option2              VARCHAR(1000) NULL,
      projteam_pager                VARCHAR(100)  NULL,
      projteam_phone                VARCHAR(100)  NULL,
      projteam_project_id           VARCHAR(100)  NOT NULL,
      projteam_regn_id              VARCHAR(100)  NULL,
      projteam_source_table         VARCHAR(100)  NULL,
      projteam_state_id             VARCHAR(50)   NULL,
      projteam_status               VARCHAR(50)   NULL,
      projteam_zip                  VARCHAR(50)   NULL,
      md5_hash                      CHAR(32)      NULL,
      edp_last_updated_timestamp    VARCHAR(30)   NULL,
      source_system                 VARCHAR(50)   NULL,
      source_account_name           VARCHAR(50)   NULL,
      edp_update_ts                 VARCHAR(30)   NULL,
      row_hash                      CHAR(32)      NOT NULL,
      bronze_batch_id               BIGINT        NOT NULL,
      bronze_load_ts                DATETIME2(0)  NOT NULL,
      CONSTRAINT PK_bronze_projteam PRIMARY KEY CLUSTERED ({paste(PRIMARY_KEY, collapse = ', ')}, bronze_load_ts)
    );"
  )
  dbExecute(con, sql)
}

# Initial Setup ####
# data <- raw_data |>
#   purrr::pluck("data")
#
# tracked_cols <- get_tracked_cols(data)
#
# hashed <- add_row_hash(data, tracked_cols) |>
#   mutate(
#     bronze_load_ts = as.POSIXct(task_start, tz = "UTC"),
#     bronze_batch_id = BATCH_ID
#   )
#
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
        status_col = NULL,
        task_start,
        BATCH_ID
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

#     already_tombstoned <- DBI::dbGetQuery(
#       con,
#       glue::glue_sql(
#         "
#   SELECT primary_key_value FROM {`AUDIT_SCHEMA`}.key_tombstones
#   WHERE source_table = {TABLE_NAME}
# ",
#         .con = con
#       )
#     ) |>
#       pull(primary_key_value)
#
#     newly_missing <- missing_active |>
#       filter(!(.data[[PRIMARY_KEY]] %in% already_tombstoned))
#
#     if (nrow(newly_missing) > 0) {
#       tombstone_rows <- newly_missing |>
#         transmute(
#           source_table = TABLE_NAME,
#           primary_key_value = as.character(.data[[PRIMARY_KEY]]),
#           last_status = bl_status,
#           detected_missing_ts = as.POSIXct(task_start, tz = "UTC"),
#           batch_id = BATCH_ID
#         )
#       DBI::dbAppendTable(con, TOMBSTONE_TABLE, tombstone_rows)
#     }

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
