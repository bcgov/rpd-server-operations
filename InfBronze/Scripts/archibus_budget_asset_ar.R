#' @title archibus_budget_asset_ar
#'
#' @description
#' Loads budget asset allocation records from the CBRE API into the Bronze
#' layer. The script extracts budget allocations associated with real-property
#' assets and cost categories, computes row-level hashes to detect changes,
#' classifies incoming records as new, changed, or unchanged, and appends
#' historical versions to support Bronze layer auditing and change tracking.
#'
#' @database BuildingIntelligence
#' @schema InfBronze
#' @table archibus_budget_asset_ar
#'
#' @source CBRE API archibus_budget_asset_ar entity
#'
#' @load_type Incremental append with hash-based change detection
#' @grain One row per budget, asset, and cost-category allocation version per bronze load timestamp
#'
#' @filters Records are retrieved only for the configured ETL extraction window
#' @filters Technical metadata fields are excluded from row-hash calculations
#'   to avoid false-positive change detection
#' @filters Missing records from the current source extract are identified and
#'   logged separately for downstream processing
#' @filters Missing records are identified solely by key comparison because
#'   the source entity does not provide a status field for missing-record handling
#'
#' @consumers TODO
#'
#' @column budget_asset_ar_budget_id Budget identifier
#' @column budget_asset_ar_asset_id Asset identifier
#' @column budget_asset_ar_ar_cost_cat Cost category associated with the allocation
#' @column budget_asset_ar_amt_total Total allocated amount
#' @column row_hash MD5 hash of tracked business attributes used for change detection
#' @column bronze_batch_id ETL batch identifier for the load execution
#' @column bronze_load_ts UTC timestamp when the Bronze record version was loaded
#'
#' @notes
#' This entity allocates budgeted costs to individual assets, properties,
#' buildings, leases, and cost categories.
#'
#' Historical versions are retained through the composite primary key of
#' (budget_asset_ar_budget_id, budget_asset_ar_asset_id,
#' budget_asset_ar_ar_cost_cat, bronze_load_ts).
#'
#' @owner David Rattray
#' @last_reviewed 2026-11-20

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
TABLE_NAME <- "archibus_budget_asset_ar"
CBRE_TABLE_NAME <- "archibus_budget_asset_ar"
PRIMARY_KEY <- c(
  "budget_asset_ar_budget_id",
  "budget_asset_ar_asset_id",
  "budget_asset_ar_ar_cost_cat"
) # might need to use budget_asset_ar_autonumber instead, but it seems like its about to run out of numbers.
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
  "budget_asset_ar_budget_id", # natural key itself — not a "value" to hash
  "budget_asset_ar_asset_id", # natural key itself — not a "value" to hash
  "budget_asset_ar_ar_cost_cat", # natural key itself — not a "value" to hash
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
      budget_asset_ar_amt_land           VARCHAR(50)  NULL,
      budget_asset_ar_amt_parking        VARCHAR(50)  NULL,
      budget_asset_ar_amt_space          VARCHAR(50)  NULL,
      budget_asset_ar_amt_total          VARCHAR(50)  NULL,
      budget_asset_ar_ar_cost_cat        VARCHAR(100) NOT NULL,
      budget_asset_ar_area_land          VARCHAR(50)  NULL,
      budget_asset_ar_area_space         VARCHAR(50)  NULL,
      budget_asset_ar_asset_id           VARCHAR(100) NOT NULL,
      budget_asset_ar_autonumber         VARCHAR(50)  NULL,
      budget_asset_ar_bl_id              VARCHAR(50)  NULL,
      budget_asset_ar_budget_id          VARCHAR(50)  NOT NULL,
      budget_asset_ar_cost_per_land      VARCHAR(50)  NULL,
      budget_asset_ar_cost_per_parking   VARCHAR(50)  NULL,
      budget_asset_ar_cost_per_space     VARCHAR(50)  NULL,
      budget_asset_ar_is_admin           VARCHAR(50)  NULL,
      budget_asset_ar_ls_id              VARCHAR(100) NULL,
      budget_asset_ar_parking_stalls     VARCHAR(50)  NULL,
      budget_asset_ar_pr_id              VARCHAR(50)  NULL,
      md5_hash                           CHAR(32)     NULL,
      source_system                      VARCHAR(50)  NULL,
      source_account_name                VARCHAR(50)  NULL,
      edp_last_updated_timestamp         VARCHAR(100) NULL,
      edp_update_ts                      VARCHAR(30)  NULL,
      row_hash                           CHAR(32)     NOT NULL,
      bronze_batch_id                    BIGINT       NOT NULL,
      bronze_load_ts                     DATETIME2(0) NOT NULL,
      CONSTRAINT PK_bronze_budget_asset_ar PRIMARY KEY CLUSTERED ({paste(PRIMARY_KEY, collapse = ', ')}, bronze_load_ts)
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
#     budget_asset_ar_budget_id,
#     budget_asset_ar_asset_id,
#     budget_asset_ar_ar_cost_cat
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

# Check Missing Data ####
# Values in the database that don't exist in current API load
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
