#' @title Archibus Project
#'
#' @description
#' Loads project records from the CBRE API into the Bronze layer. The script
#' extracts capital-planning, facility-improvement, and project-management
#' data, computes row-level hashes to detect changes, classifies incoming
#' records as new, changed, or unchanged, and appends historical versions to
#' support Bronze layer auditing and change tracking.
#'
#' @database BuildingIntelligence
#' @schema InfBronze
#' @table archibus_project
#'
#' @source CBRE API archibus_project entity
#'
#' @load_type Incremental append with hash-based change detection
#' @grain One row per project record version per bronze load timestamp
#'
#' @filters Records are retrieved only for the configured ETL extraction window
#' @filters Technical metadata fields are excluded from row-hash calculations
#'   to avoid false-positive change detection
#' @filters Source update timestamps are excluded from hash calculations
#'   because they may change without a material business-data change
#' @filters Missing records from the current source extract are identified and
#'   logged separately for downstream processing
#'
#' @consumers TODO
#'
#' @column project_project_id_key Unique project identifier
#' @column project_project_name Project name or title
#' @column project_project_num Business project number
#' @column project_status Current project lifecycle status
#' @column project_project_type Project classification or type
#' @column project_bl_id Associated building identifier
#' @column project_pr_id Associated property identifier
#' @column row_hash MD5 hash of tracked business attributes used for change detection
#' @column bronze_batch_id ETL batch identifier for the load execution
#' @column bronze_load_ts UTC timestamp when the Bronze record version was loaded
#'
#' @notes
#' Data is sourced from the CBRE API rather than directly from a database
#' table.
#'
#' Project records contain planning, budgeting, approval, scheduling,
#' property-association, and project-performance information used to manage
#' real-property initiatives, capital improvements, tenant improvements,
#' maintenance projects, and other facilities-related work.
#'
#' A hash-gating process classifies rows as New, Changed, or Unchanged before
#' loading. Historical versions are retained through the primary key of
#' (project_project_id_key, bronze_load_ts).
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
TABLE_NAME <- "archibus_project"
CBRE_TABLE_NAME <- "archibus_project"
PRIMARY_KEY <- "project_project_id_key"
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
  "project_project_id_key", # natural key itself — not a "value" to hash
  "md5_hash", # partner-supplied hash — not used
  "edp_last_updated_timestamp",
  "edp_update_ts",
  "source_system",
  "source_account_name",
  "project_source_time_update", # source-side update timestamp; can tick
  "project_source_date_update" # without any tracked field actually changing
)

# dbRemoveTable(con, TARGET_TABLE)
if (!dbExistsTable(con, TARGET_TABLE)) {
  sql <- glue::glue(
    "CREATE TABLE
    {SCHEMA_NAME}.{TABLE_NAME}
    (
      project_ac_id                    VARCHAR(100)  NULL,
      project_actions_locked           VARCHAR(50)   NULL,
      project_age_existing             VARCHAR(50)   NULL,
      project_annual_fin_exceed_amt    VARCHAR(50)   NULL,
      project_apprv_mgr1               VARCHAR(100)  NULL,
      project_apprv_mgr1_status        VARCHAR(50)   NULL,
      project_apprv_mgr2               VARCHAR(100)  NULL,
      project_apprv_mgr2_status        VARCHAR(50)   NULL,
      project_apprv_mgr3               VARCHAR(100)  NULL,
      project_apprv_mgr3_status        VARCHAR(50)   NULL,
      project_area_affected            VARCHAR(50)   NULL,
      project_benefit                  VARCHAR(1000) NULL,
      project_bl_id                    VARCHAR(100)  NULL,
      project_clientprog               VARCHAR(100)  NULL,
      project_clientprog_yn            VARCHAR(50)   NULL,
      project_comments                 VARCHAR(2000) NULL,
      project_contact_id               VARCHAR(100)  NULL,
      project_cost_bestcase            VARCHAR(50)   NULL,
      project_cost_budget              VARCHAR(50)   NULL,
      project_cost_committed           VARCHAR(50)   NULL,
      project_cost_est_baseline        VARCHAR(50)   NULL,
      project_cost_est_design          VARCHAR(50)   NULL,
      project_cost_likely              VARCHAR(50)   NULL,
      project_cost_mgmt_res            VARCHAR(50)   NULL,
      project_cost_neg_chgs            VARCHAR(50)   NULL,
      project_cost_negotiated          VARCHAR(50)   NULL,
      project_cost_paid                VARCHAR(50)   NULL,
      project_cost_target_cur          VARCHAR(50)   NULL,
      project_cost_target_orig         VARCHAR(50)   NULL,
      project_cost_worstcase           VARCHAR(50)   NULL,
      project_criticality              VARCHAR(100)  NULL,
      project_date_accepted            VARCHAR(30)   NULL,
      project_date_app_mgr1            VARCHAR(30)   NULL,
      project_date_app_mgr2            VARCHAR(30)   NULL,
      project_date_app_mgr3            VARCHAR(30)   NULL,
      project_date_approved            VARCHAR(30)   NULL,
      project_date_baseline            VARCHAR(30)   NULL,
      project_date_bids_award          VARCHAR(30)   NULL,
      project_date_closed              VARCHAR(30)   NULL,
      project_date_commence_work       VARCHAR(30)   NULL,
      project_date_completed           VARCHAR(30)   NULL,
      project_date_cont_end            VARCHAR(30)   NULL,
      project_date_created             VARCHAR(30)   NULL,
      project_date_design_com          VARCHAR(30)   NULL,
      project_date_end                 VARCHAR(30)   NULL,
      project_date_expiry              VARCHAR(30)   NULL,
      project_date_issued              VARCHAR(30)   NULL,
      project_date_requested           VARCHAR(30)   NULL,
      project_date_settlement          VARCHAR(30)   NULL,
      project_date_start               VARCHAR(30)   NULL,
      project_date_target_end          VARCHAR(30)   NULL,
      project_days_per_week            VARCHAR(50)   NULL,
      project_dept_contact             VARCHAR(100)  NULL,
      project_description              VARCHAR(5000) NULL,
      project_doc                      VARCHAR(1000) NULL,
      project_doc_acts_xfer            VARCHAR(1000) NULL,
      project_doc_charter              VARCHAR(1000) NULL,
      project_doc_image                VARCHAR(1000) NULL,
      project_doc_impl_plan            VARCHAR(1000) NULL,
      project_doc_risk_mgmt            VARCHAR(1000) NULL,
      project_doc_scorecard            VARCHAR(1000) NULL,
      project_dp_id                    VARCHAR(100)  NULL,
      project_duration                 VARCHAR(50)   NULL,
      project_duration_est             VARCHAR(50)   NULL,
      project_dv_id                    VARCHAR(100)  NULL,
      project_emp_affected             VARCHAR(50)   NULL,
      project_funding_probability      VARCHAR(50)   NULL,
      project_hierarchy_ids            VARCHAR(1000) NULL,
      project_hours_actual             VARCHAR(50)   NULL,
      project_hours_est                VARCHAR(50)   NULL,
      project_is_template              VARCHAR(50)   NULL,
      project_life_expectancy          VARCHAR(50)   NULL,
      project_ls_id                    VARCHAR(100)  NULL,
      project_multi_client             VARCHAR(50)   NULL,
      project_one_time_exceed_amt      VARCHAR(50)   NULL,
      project_option1                  VARCHAR(1000) NULL,
      project_option2                  VARCHAR(1000) NULL,
      project_payment_option           VARCHAR(50)   NULL,
      project_pct_const_comp           VARCHAR(50)   NULL,
      project_pct_design_comp          VARCHAR(50)   NULL,
      project_pct_total_comp           VARCHAR(50)   NULL,
      project_phone_dept_contact       VARCHAR(100)  NULL,
      project_phone_req                VARCHAR(100)  NULL,
      project_pin                      VARCHAR(100)  NULL,
      project_pr_id                    VARCHAR(100)  NULL,
      project_priority                 VARCHAR(50)   NULL,
      project_prob_type                VARCHAR(100)  NULL,
      project_program_id               VARCHAR(100)  NULL,
      project_proj_mgr                 VARCHAR(100)  NULL,
      project_project_id               VARCHAR(100)  NULL,
      project_project_id_key           VARCHAR(50)   NOT NULL,
      project_project_name             VARCHAR(250)  NULL,
      project_project_num              VARCHAR(100)  NULL,
      project_project_quest            VARCHAR(1000) NULL,
      project_project_return           VARCHAR(50)   NULL,
      project_project_roi              VARCHAR(50)   NULL,
      project_project_type             VARCHAR(100)  NULL,
      project_requestor                VARCHAR(100)  NULL,
      project_scope                    VARCHAR(4000) NULL,
      project_score                    VARCHAR(50)   NULL,
      project_sequence                 VARCHAR(50)   NULL,
      project_signature_ld             VARCHAR(50)   NULL,
      project_signature_tn             VARCHAR(50)   NULL,
      project_site_id                  VARCHAR(100)  NULL,
      project_source_date_update       VARCHAR(30)   NULL,
      project_source_feed_comments     VARCHAR(1000) NULL,
      project_source_record_id         VARCHAR(100)  NULL,
      project_source_status            VARCHAR(50)   NULL,
      project_source_system_id         VARCHAR(100)  NULL,
      project_source_table             VARCHAR(100)  NULL,
      project_source_time_update       VARCHAR(30)   NULL,
      project_status                   VARCHAR(100)  NULL,
      project_summary                  VARCHAR(1000) NULL,
      project_ti_service_exceed_amt    VARCHAR(50)   NULL,
      project_wsi_kahua                VARCHAR(1000) NULL,
      md5_hash                         CHAR(32)      NULL,
      edp_last_updated_timestamp       VARCHAR(30)   NULL,
      source_system                    VARCHAR(50)   NULL,
      source_account_name              VARCHAR(50)   NULL,
      edp_update_ts                    VARCHAR(30)   NULL,
      row_hash                         CHAR(32)      NOT NULL,
      bronze_batch_id                  BIGINT        NOT NULL,
      bronze_load_ts                   DATETIME2(0)  NOT NULL,
      CONSTRAINT PK_bronze_project PRIMARY KEY CLUSTERED ({PRIMARY_KEY}, bronze_load_ts)
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
        status_col = "project_status",
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
