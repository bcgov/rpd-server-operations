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
TABLE_NAME <- "archibus_cost_tran_sched"
CBRE_TABLE_NAME <- "archibus_cost_tran_sched"
PRIMARY_KEY <- c("cost_tran_sched_cost_tran_sched_id")
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
  "cost_tran_sched_cost_tran_sched_id", # natural key value — not a "value" to hash
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
      cost_tran_sched_ac_id                             VARCHAR(100) NULL,
      cost_tran_sched_activity_log_id                   VARCHAR(100) NULL,
      cost_tran_sched_amount_expense                    VARCHAR(50)  NULL,
      cost_tran_sched_amount_expense_base_budget        VARCHAR(50)  NULL,
      cost_tran_sched_amount_expense_base_payment       VARCHAR(50)  NULL,
      cost_tran_sched_amount_expense_total_payment      VARCHAR(50)  NULL,
      cost_tran_sched_amount_expense_vat_budget         VARCHAR(50)  NULL,
      cost_tran_sched_amount_expense_vat_payment        VARCHAR(50)  NULL,
      cost_tran_sched_amount_income                     VARCHAR(50)  NULL,
      cost_tran_sched_amount_income_base_budget         VARCHAR(50)  NULL,
      cost_tran_sched_amount_income_base_payment        VARCHAR(50)  NULL,
      cost_tran_sched_amount_income_total_payment       VARCHAR(50)  NULL,
      cost_tran_sched_amount_income_vat_budget          VARCHAR(50)  NULL,
      cost_tran_sched_amount_income_vat_payment         VARCHAR(50)  NULL,
      cost_tran_sched_amount_tax_late1                  VARCHAR(50)  NULL,
      cost_tran_sched_amount_tax_late2                  VARCHAR(50)  NULL,
      cost_tran_sched_amount_tax_late3                  VARCHAR(50)  NULL,
      cost_tran_sched_ba_id                             VARCHAR(50)  NULL,
      cost_tran_sched_bl_id                             VARCHAR(50)  NULL,
      cost_tran_sched_cam_cost                          VARCHAR(50)  NULL,
      cost_tran_sched_connector_id                      VARCHAR(100) NULL,
      cost_tran_sched_cost_cat_id                       VARCHAR(100) NULL,
      cost_tran_sched_cost_tran_recur_id                VARCHAR(100) NULL,
      cost_tran_sched_cost_tran_sched_id                VARCHAR(100) NOT NULL,
      cost_tran_sched_ctry_id                           VARCHAR(50)  NULL,
      cost_tran_sched_currency_budget                   VARCHAR(50)  NULL,
      cost_tran_sched_currency_payment                  VARCHAR(50)  NULL,
      cost_tran_sched_date_assessed                     VARCHAR(30)  NULL,
      cost_tran_sched_date_due                          VARCHAR(30)  NULL,
      cost_tran_sched_date_paid                         VARCHAR(30)  NULL,
      cost_tran_sched_date_tax_late1                    VARCHAR(30)  NULL,
      cost_tran_sched_date_tax_late2                    VARCHAR(30)  NULL,
      cost_tran_sched_date_tax_late3                    VARCHAR(30)  NULL,
      cost_tran_sched_date_trans_created                VARCHAR(30)  NULL,
      cost_tran_sched_date_used_for_mc_budget           VARCHAR(30)  NULL,
      cost_tran_sched_date_used_for_mc_payment          VARCHAR(30)  NULL,
      cost_tran_sched_description                       VARCHAR(1000)NULL,
      cost_tran_sched_dp_id                             VARCHAR(50)  NULL,
      cost_tran_sched_dv_id                             VARCHAR(50)  NULL,
      cost_tran_sched_entered_by                        VARCHAR(100) NULL,
      cost_tran_sched_exchange_rate_budget              VARCHAR(50)  NULL,
      cost_tran_sched_exchange_rate_override            VARCHAR(50)  NULL,
      cost_tran_sched_exchange_rate_payment             VARCHAR(50)  NULL,
      cost_tran_sched_finanal_id                        VARCHAR(50)  NULL,
      cost_tran_sched_funding_type                      VARCHAR(100) NULL,
      cost_tran_sched_gl_code                           VARCHAR(100) NULL,
      cost_tran_sched_import_source                     VARCHAR(100) NULL,
      cost_tran_sched_ls_id                             VARCHAR(100) NULL,
      cost_tran_sched_offset_gl                         VARCHAR(100) NULL,
      cost_tran_sched_option1                           VARCHAR(50)  NULL,
      cost_tran_sched_option2                           VARCHAR(50)  NULL,
      cost_tran_sched_pa_name                           VARCHAR(100) NULL,
      cost_tran_sched_parcel_id                         VARCHAR(100) NULL,
      cost_tran_sched_pr_id                             VARCHAR(50)  NULL,
      cost_tran_sched_project_id                        VARCHAR(100) NULL,
      cost_tran_sched_remit_to                          VARCHAR(200) NULL,
      cost_tran_sched_status                            VARCHAR(50)  NULL,
      cost_tran_sched_tax_authority_contact             VARCHAR(200) NULL,
      cost_tran_sched_tax_bill_num                      VARCHAR(300) NULL,
      cost_tran_sched_tax_clr                           VARCHAR(50)  NULL,
      cost_tran_sched_tax_milage_rate                   VARCHAR(50)  NULL,
      cost_tran_sched_tax_period_in_months              VARCHAR(50)  NULL,
      cost_tran_sched_tax_type                          VARCHAR(50)  NULL,
      cost_tran_sched_tax_value_assessed                VARCHAR(50)  NULL,
      cost_tran_sched_vat_amount_override               VARCHAR(50)  NULL,
      cost_tran_sched_vat_percent_override              VARCHAR(50)  NULL,
      cost_tran_sched_vat_percent_value                 VARCHAR(50)  NULL,
      cost_tran_sched_vendor                            VARCHAR(100) NULL,
      md5_hash                                          CHAR(32)     NULL,
      edp_last_updated_timestamp                        VARCHAR(100) NULL,
      source_system                                     VARCHAR(50)  NULL,
      source_account_name                               VARCHAR(50)  NULL,
      edp_update_ts                                     VARCHAR(30)  NULL,
      row_hash                                          CHAR(32)     NOT NULL,
      bronze_batch_id                                   BIGINT       NOT NULL,
      bronze_load_ts                                    DATETIME2(0) NOT NULL,
      CONSTRAINT PK_bronze_cost_tran_sched PRIMARY KEY CLUSTERED ({paste(PRIMARY_KEY, collapse = ', ')}, bronze_load_ts)
    );"
  )
  dbExecute(con, sql)
}

# Initial Setup ####
# data <- raw_data |>
#   purrr::pluck("data")
#
# test <- data |>
#   group_by(cost_tran_recur_cost_tran_recur_id_key) |>
#   mutate(count = n()) |>
#   filter(count > 1)
# tracked_cols <- get_tracked_cols(data, EXCLUDED_FROM_HASH)
#
# hashed <- add_row_hash(data, PRIMARY_KEY, tracked_cols) |>
#   mutate(
#     bronze_load_ts = as.POSIXct(task_start, tz = "UTC"),
#     bronze_batch_id = BATCH_ID
#   )
#
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
        status_col = NULL
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
