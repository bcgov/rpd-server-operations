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
TABLE_NAME <- "archibus_dp"
CBRE_TABLE_NAME <- "archibus_dp"
PRIMARY_KEY <- "dp_dp_id"
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
  "dp_dp_id", # natural key itself — not a "value" to hash
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
      dp_address_ref               VARCHAR(100) NULL,
      dp_admin_email               VARCHAR(100) NULL,
      dp_admin_phone               VARCHAR(100) NULL,
      dp_appropriated              VARCHAR(50)  NULL,
      dp_approving_mgr             VARCHAR(100) NULL,
      dp_area_avg_em               VARCHAR(50)  NULL,
      dp_area_chargable            VARCHAR(50)  NULL,
      dp_area_comn                 VARCHAR(50)  NULL,
      dp_area_comn_gp              VARCHAR(50)  NULL,
      dp_area_comn_nocup           VARCHAR(50)  NULL,
      dp_area_comn_ocup            VARCHAR(50)  NULL,
      dp_area_comn_rm              VARCHAR(50)  NULL,
      dp_area_comn_serv            VARCHAR(50)  NULL,
      dp_area_gp                   VARCHAR(50)  NULL,
      dp_area_nocup                VARCHAR(50)  NULL,
      dp_area_ocup                 VARCHAR(50)  NULL,
      dp_area_rm                   VARCHAR(50)  NULL,
      dp_area_rm_personnel         VARCHAR(50)  NULL,
      dp_area_second_circ          VARCHAR(50)  NULL,
      dp_collector                 VARCHAR(100) NULL,
      dp_contact_id                VARCHAR(100) NULL,
      dp_cost                      VARCHAR(50)  NULL,
      dp_count_em                  VARCHAR(50)  NULL,
      dp_customer_category         VARCHAR(100) NULL,
      dp_customer_class            VARCHAR(100) NULL,
      dp_customer_ref              VARCHAR(100) NULL,
      dp_customer_segment          VARCHAR(100) NULL,
      dp_dp_id                     VARCHAR(100) NOT NULL,
      dp_dv_id                     VARCHAR(50)  NULL,
      dp_em_area_chargable         VARCHAR(50)  NULL,
      dp_em_area_comn              VARCHAR(50)  NULL,
      dp_em_area_comn_rm           VARCHAR(50)  NULL,
      dp_em_area_comn_serv         VARCHAR(50)  NULL,
      dp_em_area_rm                VARCHAR(50)  NULL,
      dp_em_cost                   VARCHAR(50)  NULL,
      dp_fee_recovery              VARCHAR(50)  NULL,
      dp_gl_code                   VARCHAR(100) NULL,
      dp_head                      VARCHAR(100) NULL,
      dp_hpattern                  VARCHAR(100) NULL,
      dp_hpattern_acad             VARCHAR(100) NULL,
      dp_mvpt_category             VARCHAR(100) NULL,
      dp_name                      VARCHAR(500) NULL,
      dp_name_short                VARCHAR(100) NULL,
      dp_option1                   VARCHAR(50)  NULL,
      dp_option2                   VARCHAR(50)  NULL,
      dp_pam                       VARCHAR(50)  NULL,
      dp_reconciled                VARCHAR(50)  NULL,
      dp_recovery_fee              VARCHAR(50)  NULL,
      dp_sales_rep                 VARCHAR(100) NULL,
      dp_source_record_id          VARCHAR(100) NULL,
      dp_status                    VARCHAR(50)  NULL,
      dp_tax_code                  VARCHAR(50)  NULL,
      dp_upload_charge             VARCHAR(50)  NULL,
      dp_uuid                      VARCHAR(100) NULL,
      md5_hash                     CHAR(32)     NULL,
      edp_last_updated_timestamp   VARCHAR(100) NULL,
      source_system                VARCHAR(50)  NULL,
      source_account_name          VARCHAR(50)  NULL,
      edp_update_ts                VARCHAR(30)  NULL,
      row_hash                     CHAR(32)     NOT NULL,
      bronze_batch_id              BIGINT       NOT NULL,
      bronze_load_ts               DATETIME2(0) NOT NULL,
      CONSTRAINT PK_bronze_dp PRIMARY KEY CLUSTERED ({PRIMARY_KEY}, bronze_load_ts)
    );"
  )
  dbExecute(con, sql)
}

# Initial Setup ####
data <- raw_data |>
  purrr::pluck("data")

tracked_cols <- get_tracked_cols(data, EXCLUDED_FROM_HASH)

hashed <- add_row_hash(data, PRIMARY_KEY, tracked_cols) |>
  mutate(
    bronze_load_ts = as.POSIXct(task_start, tz = "UTC"),
    bronze_batch_id = BATCH_ID
  )
str(hashed, max.level = 2, vec.len = 0, list.len = Inf)
max_char_lengths(hashed)
# DBI::dbAppendTable(con, TARGET_TABLE, hashed)
test <- hashed |> group_by(dp_dp_id) |> mutate(count = n()) |> filter(count > 1)

output <- test |>
  select(
    dp_name,
    dp_hpattern_acad,
    dp_dp_id,
    dp_dv_id,
    dp_customer_category,
    dp_collector,
    dp_sales_rep
  )

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
        status_col = "rmpct_status_pobc"
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
